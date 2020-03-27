#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import enum
import logging
import os
import threading
from contextlib import contextmanager
from datetime import timedelta
from functools import total_ordering, wraps
from io import BytesIO
from pathlib import Path, PurePath
from typing import (
    IO,
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
    Type)

from nanotime import nanotime

from hashkernel import (
    BitMask,
    CodeEnum,
    EnsureIt,
    GlobalRef,
    Primitive,
    ScaleHelper,
    Scaling,
    Stringable,
    MetaCodeEnumExtended)
from hashkernel.base_x import base_x
from hashkernel.files import ensure_path
from hashkernel.hashing import (
    NULL_HASH_KEY,
    SIZE_OF_HASH_KEY,
    B36_Mixin,
    BytesOrderingMixin,
    Hasher,
    HashKey,
)
from hashkernel.packer import (
    INT_8,
    NANOTIME,
    FixedSizePacker,
    Packer,
    PackerLibrary,
    ProxyPacker,
    TuplePacker,
    build_code_enum_packer,
)
from hashkernel.plugins import query_plugins
from hashkernel.smattr import BytesWrap, JsonWrap, SmAttr, build_named_tuple_packer
from hashkernel.time import (
    FOREVER_DELTA,
    M_1,
    NANO_TTL_PACKER,
    TTL,
    TTL_PACKER,
    W_1,
    Y_1,
    Timeout,
    d_1,
    d_4,
    h_1,
    nano_ttl,
    nanotime_now,
)

log = logging.getLogger(__name__)



B62 = base_x(62)

LINK_SECONDS = [h_1, d_1, d_4, W_1, M_1, Y_1, Y_1 * 10]


class LinkTimeout(Timeout):
    @staticmethod
    def __new_scale_helper__():
        return ScaleHelper(
            lambda i: FOREVER_DELTA if i == 7 else timedelta(seconds=LINK_SECONDS[i]),
            bit_size=3,
        )


HOUR_LT = LinkTimeout(LINK_SECONDS.index(h_1))
DAY_LT = LinkTimeout(LINK_SECONDS.index(d_4))
WEEK_LT = LinkTimeout(LINK_SECONDS.index(W_1))
MONTH_LT = LinkTimeout(LINK_SECONDS.index(M_1))
YEAR_LT = LinkTimeout(LINK_SECONDS.index(Y_1))
DECADE_LT = LinkTimeout(len(LINK_SECONDS) - 1)
FOREVER_LT = LinkTimeout(len(LINK_SECONDS))


SIZES = [1, 10, 1000, 100000]


class LinkHistorySize(Scaling):
    @staticmethod
    def __new_scale_helper__():
        return ScaleHelper(lambda i: SIZES[i], bit_size=2)


KEEP_ONE, KEEP_TEN, KEEP_THOUSAND, KEEP_ALL = LinkHistorySize.all()

IDX_MASK = BitMask(0, 3)
SIZE_MASK = BitMask(3, 2)
TIMEOUT_MASK = BitMask(5, 3)


class LinkIdx:
    idx: int  # 3 bits
    size: LinkHistorySize  # 2 bits
    timeout: LinkTimeout  # 3 bits

    def __init__(
        self,
        idx: int,
        size: Optional[LinkHistorySize] = None,
        timeout: Optional[LinkTimeout] = None,
    ):
        if size is None and timeout is None:
            self.idx = IDX_MASK.extract(idx)
            self.size = LinkHistorySize(SIZE_MASK.extract(idx))
            self.timeout = LinkTimeout(TIMEOUT_MASK.extract(idx))
        else:
            assert (
                size is not None and timeout is not None
            ), "size and timeout both has to be defined"
            assert (
                not IDX_MASK.inverse & idx
            ), "size and timeout bits in idx has to be zeroed"
            self.idx = idx
            self.size = size
            self.timeout = timeout

    def __int__(self):
        return BitMask.update_all(
            0,
            (IDX_MASK, self.idx),
            (SIZE_MASK, self.size),
            (TIMEOUT_MASK, self.timeout),
        )

    def __repr__(self):
        return f"LinkIdx({self.idx}, {self.size}, {repr(self.timeout)})"


class LinkType(NamedTuple):
    name: str
    idx: LinkIdx
    ref: Optional[GlobalRef] = None


class RakeLinks:
    """
    >>> cl = RakeLinks()
    >>> cl.add_links(('a',0 , KEEP_ONE, HOUR_LT, None))
    >>> cl.add_links(('b',1 , KEEP_ONE, HOUR_LT, None))
    >>> cl.add_links(('c',1 , KEEP_ONE, HOUR_LT, None))
    Traceback (most recent call last):
    ...
    AssertionError: Suggested idx: 0x02
    >>> cl.add_links(('c', 0x02, KEEP_ONE, HOUR_LT, None),
    ...     ('d', 3, KEEP_ONE, HOUR_LT, None),
    ...     ('e', 4, KEEP_ONE, HOUR_LT, None),
    ...     ('f', 5, KEEP_ONE, HOUR_LT, None),
    ...     ('h', 6, KEEP_ONE, HOUR_LT, None))
    ...
    >>> cl.add_links(('c', 7 , KEEP_ONE, HOUR_LT, None))
    Traceback (most recent call last):
    ...
    AssertionError: Duplicate name: c
    >>> cl.add_links(('g', 7, KEEP_ONE, HOUR_LT, None))
    >>> cl.add_links(('h', 8, KEEP_ONE, HOUR_LT, None))
    Traceback (most recent call last):
    ...
    AssertionError: No slots
    >>> cl.add_links(('x',8 , KEEP_THOUSAND, DECADE_LT, None))
    Traceback (most recent call last):
    ...
    AssertionError: Expected idx: 0xd0
    >>> cl.add_links(('x',0xd0 , KEEP_THOUSAND, DECADE_LT, None))
    >>> cl.links_by_name['x']
    LinkType(name='x', idx=LinkIdx(0, LinkHistorySize(2), LinkTimeout(6)), ref=None)

    """

    links_by_idx: Dict[int, LinkType]
    links_by_name: Dict[str, LinkType]

    def __init__(self, *links: Tuple[str, int, LinkHistorySize, LinkTimeout, Optional[GlobalRef]]):
        self.links_by_idx = {}
        self.links_by_name = {}
        if len(links):
            self.add_links(*links)

    def add_links(
        self, *links: Tuple[str, int, LinkHistorySize, LinkTimeout, Optional[GlobalRef]]
    ):
        for name, idx_check, size, timeout, type_gref in links:
            i = IDX_MASK.extract(idx_check)
            for add in range(8):
                link_idx = LinkIdx(i + add, size, timeout)
                idx = int(link_idx)
                unique_idx = idx not in self.links_by_idx
                if unique_idx:
                    break
            assert unique_idx, "No slots"
            assert add == 0, f"Suggested idx: {idx:#04x}"
            assert name not in self.links_by_name, f"Duplicate name: {name}"
            assert idx == idx_check, f"Expected idx: {idx:#04x}"
            link_type = LinkType(name, link_idx, type_gref)
            self.links_by_name[name] = link_type
            self.links_by_idx[idx] = link_type


OBJ_TYPE_MASK = BitMask(0, 6)
RAKE_SIZEOF = 16

@total_ordering
class Rake(Stringable, B36_Mixin, BytesOrderingMixin):
    """
    RAndom KEy
    16 bytes - simular to UUIDv4, same collision likelyhood
    122bit - urandom component
    6bit - object type defined by schema. Packed in lower bits of last byte

    >>> r0 = Rake.build_new(0)
    >>> Rake(str(r0)) == r0
    True
    >>> r0_b36 = Rake.from_b36(r0.to_b36())
    >>> r0 == r0_b36
    True
    >>> Rake.build_new(-1)
    Traceback (most recent call last):
    ...
    AssertionError: out of range 0-63: -1
    >>> Rake.build_new(64)
    Traceback (most recent call last):
    ...
    AssertionError: out of range 0-63: 64
    >>> ru = list(map( Rake.build_new, range(64)))
    >>> all(ru[i].obj_type() == i for i in range(64))
    True
    >>> rs = sorted( ru )
    >>> rs != ru
    True
    >>> rss = sorted(map( lambda r: Rake(str(r)), ru))
    >>> rs == rss
    True
    >>> all(rs[i] < rs[i+1] for i in range(63))
    True
    >>> any(rs[i] > rs[i+1] for i in range(63))
    False
    >>> all(hash(rs[i]) == hash(rss[i]) for i in range(64))
    True
    >>> r0 > r0_b36
    False
    >>> Rake('32GweQDJvoH9dtuHzBGk6s')
    Rake('32GweQDJvoH9dtuHzBGk6s')
    >>> Rake.null(0)
    Rake('0000000000000000')
    """

    buffer: bytes

    __packer__: ClassVar[Packer]

    def __init__(self, s: Union[str, bytes]):
        if isinstance(s, str):
            s = B62.decode(s)
        assert len(s) == RAKE_SIZEOF, f"Wrong size:{len(s)}"
        self.buffer = s

    @classmethod
    def build_new(cls, obj_type):
        return cls._build(obj_type, os.urandom(RAKE_SIZEOF))

    @classmethod
    def null(cls, obj_type):
        return cls._build(obj_type, b'\x00' * RAKE_SIZEOF)

    @classmethod
    def _build(cls, obj_type, s: bytes):
        if not isinstance(obj_type , int):
            obj_type = int(obj_type)
        assert 0 <= obj_type < 64, f"out of range 0-63: {obj_type}"
        assert len(s) == RAKE_SIZEOF
        return cls(s[:-1] + bytes([OBJ_TYPE_MASK.update(s[-1], obj_type)]))

    def obj_type(self) -> int:
        return OBJ_TYPE_MASK.extract(self.buffer[-1])

    def __bytes__(self):
        return self.buffer

    def __str__(self):
        return B62.encode(self.buffer)

    def __hash__(self) -> int:
        return hash(self.buffer)

Rake.__packer__ = ProxyPacker(Rake, FixedSizePacker(RAKE_SIZEOF))

class RakeSchema(CodeEnum):
    def __init__(
        self,
        code:int,
        links:Optional[RakeLinks] = None,
        doc:str = "",
    ):
        assert 0 <= code < 64
        CodeEnum.__init__(self, code, doc)
        self.links = links

    @classmethod
    def extends(cls, *enums: Type["RakeSchema"]):
        def decorate(decorated_enum: Type["RakeSchema"]):
            @wraps(decorated_enum)
            class CombinedRakeSchema(RakeSchema,
                                     metaclass=MetaCodeEnumExtended,
                                     enums=[cls, decorated_enum, *enums]):
                pass
            return CombinedRakeSchema
        return decorate



class RootSchema(RakeSchema):
    SCHEMA = 0
    CASKADE = 1
    HOST = 2
    ACTOR = 3
    LOGIC = 4







class HasCake(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def cake(self) -> HashKey:
        raise NotImplementedError("subclasses must override")


class HasCakeFromBytes:
    def cake(self) -> HashKey:
        return HashKey.from_bytes(bytes(self)) # type:ignore


HasCake.register(HasCakeFromBytes)


class QuestionMsg(SmAttr, HasCakeFromBytes):
    ref: GlobalRef
    data: Dict[str, Any]


class ResponseChain(SmAttr, HasCakeFromBytes):
    previous: HashKey


class DataChunkMsg(ResponseChain, HasCakeFromBytes):
    data: Any


class ResponseMsg(ResponseChain, HasCakeFromBytes):
    data: Dict[str, Any]
    traceback: Optional[str] = None

    def is_error(self):
        return self.traceback is not None


@total_ordering
class BlockStream(BytesOrderingMixin):
    """
    >>> bs = BlockStream(blocks=[NULL_HASH_KEY, NULL_HASH_KEY])
    >>> len(bytes(bs))
    64
    >>> bs == BlockStream(bytes(bs))
    True
    >>> bs != BlockStream(bytes(bs))
    False
    """

    blocks: List[HashKey]

    def __init__(
        self,
        buffer: Optional[bytes] = None,
        blocks: Optional[Iterable[HashKey]] = None,
    ):
        if buffer is not None:
            assert blocks is None
            len_of = len(buffer)
            assert len_of % SIZE_OF_HASH_KEY == 0
            self.blocks = []
            offset = 0
            for _ in range(len_of // SIZE_OF_HASH_KEY):
                end = offset + SIZE_OF_HASH_KEY
                self.blocks.append(HashKey(buffer[offset:end]))
                offset = end
        else:
            assert blocks is not None
            self.blocks = list(blocks)

    def __bytes__(self):
        return b"".join(map(bytes, self.blocks))


class HashSession(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def load_content(self, cake: HashKey) -> bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    async def store_content(
        self, content: Union[bytes, IO[bytes]]
    ) -> HashKey:
        raise NotImplementedError("subclasses must override")

    def close(self):
        pass


class HashContext:
    @staticmethod
    def get() -> HashSession:
        return threading.local().hash_ctx

    @staticmethod
    def set(ctx: HashSession):
        if ctx is None:
            try:
                del threading.local().hash_ctx
            except AttributeError:
                pass
        else:
            (threading.local()).hash_ctx = ctx

    @staticmethod
    @contextmanager
    def context(factory: Callable[[], HashSession]):
        session = factory()
        HashContext.set(session)
        try:
            yield session
        finally:
            HashContext.set(None)
            session.close()



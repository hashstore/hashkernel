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


class CakeProperties(enum.Enum):
    IS_HASH = enum.auto()
    IS_GUID = enum.auto()
    IS_FOLDER = enum.auto()
    IS_JOURNAL = enum.auto()
    IS_VTREE = enum.auto()

    def __str__(self) -> str:
        return self.name.lower()

    @staticmethod
    def set_properties(target: Any, *modifiers: "CakeProperties") -> None:
        for e in CakeProperties:
            setattr(target, str(e), e in modifiers)

    @staticmethod
    def typings() -> None:
        for e in CakeProperties:
            print(f"{e}:bool")


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





class CakeType:
    modifiers: Set[CakeProperties]
    gref: Optional[GlobalRef]
    idx: Optional[int]
    name: Optional[str]
    cake_types: Optional["CakeTypeRegistar"]

    def __init__(self, modifiers, gref=None, idx=None, name=None, cake_types=None):
        assert (
            CakeProperties.IS_GUID in modifiers or CakeProperties.IS_HASH in modifiers
        )
        self.modifiers = modifiers
        self.gref = gref
        self.idx = idx
        self.name = name
        self.cake_types = cake_types

    def update_gref(self, gref_or_type: Union[type, GlobalRef]):
        gref = GlobalRef.ensure_it(gref_or_type)
        if self.gref is None:
            self.gref = gref
        else:
            assert self.gref != gref, f"conflict gref: {self.gref} vs {gref}"
        if self.cake_types is not None:
            self.cake_types.__types__ = None

    def __int__(self):
        assert self.idx is not None
        return self.idx

    def __str__(self):
        return B62.alphabet[self.idx]

    def __bytes__(self):
        return bytes((self.idx,))


class CakeTypeRegistar(type):
    def __init__(cls, name, bases, dct):
        cls.__cake_types__ = [None for _ in range(62)]
        cls.__by_name__: Dict[str, "CakeType"] = {}
        cls.__types__: Optional[Dict[GlobalRef, "CakeType"]] = None
        idx = dct.get("__start_idx__", 0)
        for k in dct:
            if k[:1] != "_":
                if isinstance(dct[k], CakeType):
                    ch: CakeType = dct[k]
                    ch.idx = idx
                    ch.name = k
                    cls.register(ch)
                    idx += 1

    def resolve(cls, k):
        if isinstance(k, str):
            if len(k) == 1:
                k = B62.index[k]
            else:
                return cls.__by_name__[k]
        if isinstance(k, int):
            v = cls.__cake_types__[k]
            if v is not None:
                return v
        raise KeyError(k)

    def __getitem__(cls, k):
        return cls.resolve(k)

    def extend(cls, ctr: "CakeTypeRegistar"):
        for ct in ctr.cake_types():
            cls.register(ct)

    def register(cls, ch: CakeType):
        assert ch.name is not None
        assert ch.name not in cls.__by_name__
        if ch.idx is not None:
            assert cls.__cake_types__[ch.idx] is None
        else:
            ch.idx = next(i for i, h in enumerate(cls.__cake_types__) if h is None)
        cls.__cake_types__[ch.idx] = ch
        cls.__by_name__[ch.name] = ch
        ch.cake_types = cls

    def by_type(cls, gref: Union[type, GlobalRef]) -> Optional["CakeType"]:
        gref = GlobalRef.ensure_it(gref)
        if cls.__types__ is None:
            cls.__types__ = {
                h.gref: h
                for h in cls.cake_types()
                if CakeProperties.IS_HASH in h.modifiers and h.gref is not None
            }
        return cls.__types__[GlobalRef.ensure_it(gref)]

    def cake_types(cls) -> Iterable["CakeType"]:
        return (h for h in cls.__cake_types__ if h is not None)


class CakeTypes(metaclass=CakeTypeRegistar):
    NO_CLASS = CakeType({CakeProperties.IS_HASH})
    JOURNAL = CakeType({CakeProperties.IS_GUID, CakeProperties.IS_JOURNAL})
    FOLDER = CakeType({CakeProperties.IS_HASH, CakeProperties.IS_FOLDER})
    TIMESTAMP = CakeType({CakeProperties.IS_GUID})
    CASK = CakeType({CakeProperties.IS_GUID})
    BLOCKSTREAM = CakeType({CakeProperties.IS_HASH})


CAKE_TYPE_PACKER = ProxyPacker(CakeType, INT_8, int, CakeTypes.resolve)


class MsgTypes(metaclass=CakeTypeRegistar):
    """
    >>> MsgTypes.QUESTION_MSG.idx
    16
    >>> MsgTypes.QUESTION_MSG.cake_types == CakeTypes
    True
    """

    __start_idx__ = 0x10
    QUESTION_MSG = CakeType({CakeProperties.IS_HASH})
    RESPONSE_MSG = CakeType({CakeProperties.IS_HASH})
    DATA_CHUNK_MSG = CakeType({CakeProperties.IS_HASH})
    JSON_WRAP = CakeType({CakeProperties.IS_HASH}, gref=GlobalRef(JsonWrap))
    BYTES_WRAP = CakeType({CakeProperties.IS_HASH}, gref=GlobalRef(BytesWrap))
    JOURNAL_FOLDER = CakeType(
        {CakeProperties.IS_GUID, CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL}
    )
    VTREE_FOLDER = CakeType(
        {CakeProperties.IS_GUID, CakeProperties.IS_FOLDER, CakeProperties.IS_VTREE}
    )
    MOUNT_FOLDER = CakeType(
        {CakeProperties.IS_GUID, CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL}
    )
    SESSION = CakeType({CakeProperties.IS_GUID})
    NODE = CakeType(
        {CakeProperties.IS_GUID, CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL}
    )
    USER = CakeType(
        {CakeProperties.IS_GUID, CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL}
    )


CakeTypes.extend(MsgTypes)

GUIDHEADER_TUPLE = TuplePacker(NANOTIME, TTL_PACKER, INT_8)

ON_HISTORY_BIT = BitMask(0)


class GuidHeader:
    SIZEOF = GUIDHEADER_TUPLE.size

    time: nanotime
    ttl: TTL
    reserved: int

    def __init__(
        self,
        t: Union[None, bytes, nanotime],
        ttl: Optional[TTL] = None,
        reserved: int = None,
    ):

        if isinstance(t, bytes):
            assert ttl is None and reserved is None
            (self.time, self.ttl, self.reserved), _ = GUIDHEADER_TUPLE.unpack(t, 0)
        else:
            self.time = nanotime_now() if t is None else t
            self.ttl = TTL() if ttl is None else ttl
            self.reserved = 0 if reserved is None else reserved

    def ttl_on_history(self, set: Optional[bool] = None) -> bool:
        if isinstance(set, bool):
            self.ttl.set_extra_bit(ON_HISTORY_BIT, set)
        return self.ttl.get_extra_bit(ON_HISTORY_BIT)

    def __bytes__(self):
        return GUIDHEADER_TUPLE.pack((self.time, self.ttl, self.reserved))


SIZEOF_CAKE = Hasher.SIZEOF + 1
UNIFORM_DIGEST_SIZEOF = Hasher.SIZEOF - GuidHeader.SIZEOF


@total_ordering
class Cake(Stringable, EnsureIt, Primitive):
    """
    Stands for Content Address Key.

    Content addressing scheme using SHA256 hash. Base62 encoding is
    used to encode bytes.


    >>> short_content = b'The quick brown fox jumps over'
    >>> short_k = Cake.from_bytes(short_content)
    >>> str(short_k)
    'l01natqrQGg1ueJkFIc9mUYt18gcJjdsPLSLyzGgjY70'
    >>> short_k.is_guid
    False
    >>> short_k.is_hash
    True
    >>> longer_content = b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'
    >>> longer_k = Cake.from_bytes(longer_content)
    >>> str(longer_k)
    'zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT0'
    >>> len(longer_k.hash_key.digest)
    32
    >>> len({hash(longer_k), hash(Cake(str(longer_k)))})
    1
    >>> len({longer_k , Cake(str(longer_k))})
    1

    Global Unique ID can be generated, first 10 is `GuidHeader` and
    22 byte random sequence follows. It is stored and encoded in same
    way as hash.

    >>> guid = Cake.new_guid()
    >>> guid.is_guid
    True
    >>> len(str(guid))
    44

    >>> nt_before = nanotime_now()
    >>> g1 = Cake.new_guid()
    >>> gh = g1.guid_header()
    >>> nt_after = nanotime_now()
    >>> nt_before.nanoseconds() <= gh.time.nanoseconds()
    True
    >>> gh.time.nanoseconds() <= nt_after.nanoseconds()
    True

    >>> CakeProperties.typings()
    is_hash:bool
    is_guid:bool
    is_folder:bool
    is_journal:bool
    is_vtree:bool

    """

    is_hash: bool
    is_guid: bool
    is_folder: bool
    is_journal: bool
    is_vtree: bool
    hash_key: HashKey
    type: CakeType

    __packer__: ClassVar[Packer]

    def __init__(
        self,
        s: Union[str, bytes, None],
        digest: Union[HashKey, bytes, str, None] = None,
        type: Optional[CakeType] = None,
    ) -> None:
        if s is None:
            assert (
                digest is not None and type is not None
            ), f"both digest={digest!r} and type={type!r} required"
            self.hash_key = HashKey.ensure_it(digest)
            self.type = type
        elif isinstance(s, bytes):
            assert len(s) == SIZEOF_CAKE, f"invalid length of s: {len(s)}"
            self.hash_key = HashKey(s[:-1])
            self.type = CakeTypes[ord(s[-1:])]
        else:
            self.hash_key = HashKey(B62.decode(s[:-1]))
            self.type = CakeTypes[s[-1:]]
        CakeProperties.set_properties(self, *self.type.modifiers)

    def guid_header(self) -> GuidHeader:
        assert self.is_guid
        return GuidHeader(self.hash_key.digest)

    def uniform_digest(self):
        """
        in case of guid first bytes of digest contains `nano_ttl` which
        is not unifomrly distributed

        :return: Portion of digest that could be used for sharding and routing
        """
        return self.hash_key.digest[GuidHeader.SIZEOF :]

    @staticmethod
    def from_stream(fd: IO[bytes], type=CakeTypes.NO_CLASS) -> "Cake":
        assert CakeProperties.IS_HASH in type.modifiers
        return Cake(None, digest=Hasher().update_from_stream(fd).digest(), type=type)

    @staticmethod
    def from_bytes(s: bytes, type=CakeTypes.NO_CLASS) -> "Cake":
        return Cake.from_stream(BytesIO(s), type)

    @staticmethod
    def from_file(file: Union[str, Path], type=CakeTypes.NO_CLASS) -> "Cake":
        return Cake.from_stream(ensure_path(file).open("rb"), type)

    @staticmethod
    def new_guid(
        type: CakeType = CakeTypes.TIMESTAMP,
        ttl: Union[TTL, nanotime, timedelta, None] = None,
        uniform_digest: bytes = None,
    ) -> "Cake":
        ttl = TTL.ensure_it_or_none(ttl)
        if uniform_digest is None:
            uniform_digest = os.urandom(UNIFORM_DIGEST_SIZEOF)
        else:
            assert len(uniform_digest) == UNIFORM_DIGEST_SIZEOF
        digest = bytes(GuidHeader(nanotime_now(), ttl)) + uniform_digest
        return Cake(None, digest=digest, type=type)

    @staticmethod
    def from_hash_key(hash_key: Union[bytes, str, HashKey], type: CakeType):
        return Cake(None, hash_key, type)

    def assert_guid(self) -> None:
        assert self.is_guid, f"has to be a guid: {self}"

    def __str__(self) -> str:
        if not (hasattr(self, "_str")):
            self._str = B62.encode(self.hash_key.digest) + str(self.type)
        return self._str

    def __repr__(self) -> str:
        return f"Cake({str(self)!r})"

    def __hash__(self) -> int:
        return hash(self.hash_key)

    def __eq__(self, other) -> bool:
        return str(self) == str(other)

    def __lt__(self, other) -> bool:
        return str(self) < str(other)

    def __bytes__(self):
        return self.hash_key.digest + bytes(self.type)


Cake.__packer__ = ProxyPacker(Cake, FixedSizePacker(SIZEOF_CAKE))

NULL_CAKE = Cake.from_bytes(b"")


class HasCake(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def cake(self) -> Cake:
        raise NotImplementedError("subclasses must override")


class HasCakeFromBytes:
    def cake(self) -> Cake:
        return Cake.from_bytes(
            bytes(self),  # type:ignore
            type=CakeTypes.by_type(self.__class__),
        )


HasCake.register(HasCakeFromBytes)


class QuestionMsg(SmAttr, HasCakeFromBytes):
    ref: GlobalRef
    data: Dict[str, Any]


class ResponseChain(SmAttr, HasCakeFromBytes):
    previous: Cake


class DataChunkMsg(ResponseChain, HasCakeFromBytes):
    data: Any


class ResponseMsg(ResponseChain, HasCakeFromBytes):
    data: Dict[str, Any]
    traceback: Optional[str] = None

    def is_error(self):
        return self.traceback is not None


class TimedCake(NamedTuple):
    tstamp: nanotime
    cake: Cake


class TimedPath(NamedTuple):
    tstamp: nanotime
    path: PurePath
    cake: Cake


class Journal:
    history: List[TimedCake]


class VirtualTree:
    history: List[TimedPath]


def named_tuple_resolver(cls: type) -> Packer:
    return build_named_tuple_packer(cls, BAKERY_PACKERS.get_packer_by_type)


BAKERY_PACKERS = PackerLibrary().register_all(
    (Cake, Cake.__packer__),
    (HashKey, HashKey.__packer__),
    (nanotime, NANOTIME),
    (nano_ttl, NANO_TTL_PACKER),
    (CakeType, CAKE_TYPE_PACKER),
    (CodeEnum, build_code_enum_packer),
    (NamedTuple, named_tuple_resolver),
)


TIMED_CAKE_PACKER = BAKERY_PACKERS.get_packer_by_type(TimedCake)


@total_ordering
class BlockStream(BytesOrderingMixin):
    """
    >>> bs = BlockStream(blocks=[NULL_HASH_KEY, NULL_HASH_KEY])
    >>> len(bytes(bs))
    65
    >>> bs == BlockStream(bytes(bs))
    True
    >>> bs != BlockStream(bytes(bs))
    False
    """

    type: CakeType
    blocks: List[HashKey]

    def __init__(
        self,
        s: Optional[bytes] = None,
        blocks: Optional[Iterable[HashKey]] = None,
        type: CakeType = CakeTypes.NO_CLASS,
    ):
        if s is not None:
            assert blocks is None
            len_of_s = len(s)
            assert len_of_s % SIZE_OF_HASH_KEY == 1
            self.type = CakeTypes[ord(s[:1])]
            self.blocks = []
            offset = 1
            for _ in range(len_of_s // SIZE_OF_HASH_KEY):
                end = offset + SIZE_OF_HASH_KEY
                self.blocks.append(HashKey(s[offset:end]))
                offset = end
        else:
            assert blocks is not None
            self.type = type
            self.blocks = list(blocks)

    def __bytes__(self):
        return bytes(self.type) + b"".join(map(bytes, self.blocks))


CakeTypes.BLOCKSTREAM.update_gref(BlockStream)
MsgTypes.QUESTION_MSG.update_gref(QuestionMsg)
MsgTypes.RESPONSE_MSG.update_gref(ResponseMsg)
MsgTypes.DATA_CHUNK_MSG.update_gref(DataChunkMsg)


class HashSession(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def load_content(self, cake: Cake) -> bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    async def store_content(
        self, cake_type: CakeType, content: Union[bytes, IO[bytes]]
    ) -> Cake:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    async def edit_journal(self, journal: Union[Cake], content: Optional[Cake]):
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


for ctr in query_plugins(CakeTypeRegistar, "hashkernel.cake_types"):
    CakeTypes.extend(ctr)

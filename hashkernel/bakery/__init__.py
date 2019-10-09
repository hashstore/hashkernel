#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import enum
import logging
import os
import threading
from contextlib import contextmanager
from datetime import timedelta
from functools import total_ordering
from io import BytesIO
from pathlib import PurePath
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
    Union,
)

from nanotime import nanotime

from hashkernel import CodeEnum, EnsureIt, GlobalRef, OneBit, Primitive, Stringable
from hashkernel.base_x import base_x
from hashkernel.hashing import Hasher
from hashkernel.packer import (
    INT_8,
    NANOTIME,
    FixedSizePacker,
    Packer,
    ProxyPacker,
    TuplePacker,
    build_code_enum_packer,
)
from hashkernel.plugins import query_plugins
from hashkernel.smattr import BytesWrap, JsonWrap, SmAttr, build_named_tuple_packer
from hashkernel.time import NANO_TTL_PACKER, TTL, TTL_PACKER, nano_ttl, nanotime_now
from hashkernel.typings import is_NamedTuple

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
B36 = base_x(36)


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

ON_HISTORY_BIT = OneBit(0)


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
    >>> len(longer_k.digest)
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

    __packer__: ClassVar[Packer]

    def __init__(
        self,
        s: Union[str, bytes, None],
        digest: Optional[bytes] = None,
        type: Optional[CakeType] = None,
    ) -> None:
        if s is None:
            assert (
                digest is not None and type is not None
            ), f"both digest={digest} and type={type} required"
            self.digest = digest
            self.type = type
        elif isinstance(s, bytes):
            assert len(s) == SIZEOF_CAKE, f"invalid length of s: {len(s)}"
            self.digest = s[:-1]
            self.type = CakeTypes[ord(s[-1:])]
        else:
            self.digest = B62.decode(s[:-1])
            self.type = CakeTypes[s[-1:]]
        CakeProperties.set_properties(self, *self.type.modifiers)
        if len(self.digest) != Hasher.SIZEOF:
            raise AttributeError(f"invalid cake digest: {s} {digest.hex()} {type} ")

    def guid_header(self) -> GuidHeader:
        assert self.is_guid
        return GuidHeader(self.digest)

    def uniform_digest(self):
        """
        in case of guid first bytes of digest contains `nano_ttl` which
        is not unifomrly distributed

        :return: Portion of digest that could be used for sharding and routing
        """
        return self.digest[GuidHeader.SIZEOF :]

    @staticmethod
    def from_stream(fd: IO[bytes], type=CakeTypes.NO_CLASS) -> "Cake":
        assert CakeProperties.IS_HASH in type.modifiers
        return Cake(None, digest=Hasher().update_from_stream(fd).digest(), type=type)

    @staticmethod
    def from_bytes(s: bytes, type=CakeTypes.NO_CLASS) -> "Cake":
        return Cake.from_stream(BytesIO(s), type)

    @staticmethod
    def from_file(file: str, type) -> "Cake":
        return Cake.from_stream(open(file, "rb"), type)

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
    def from_digest36(digest: str, type: CakeType):
        return Cake(None, B36.decode(digest), type)

    def assert_guid(self) -> None:
        assert self.is_guid, f"has to be a guid: {self}"

    def __str__(self) -> str:
        if not (hasattr(self, "_str")):
            self._str = B62.encode(self.digest) + str(self.type)
        return self._str

    def digest36(self) -> str:
        return B36.encode(self.digest)

    def __repr__(self) -> str:
        return f"Cake({str(self)!r})"

    def __hash__(self) -> int:
        if not (hasattr(self, "_hash")):
            self._hash = hash(self.digest)
        return self._hash

    def __eq__(self, other) -> bool:
        return str(self) == str(other)

    def __le__(self, other) -> bool:
        return str(self) < str(other)

    def __bytes__(self):
        return self.digest + bytes(self.type)


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


_BAKERY_PACKERS = {
    Cake: Cake.__packer__,
    nanotime: NANOTIME,
    nano_ttl: NANO_TTL_PACKER,
    CakeType: CAKE_TYPE_PACKER,
}


def type_to_packer_resolver(cls: type) -> Packer:
    if cls in _BAKERY_PACKERS:
        return _BAKERY_PACKERS[cls]
    if issubclass(cls, CodeEnum):
        return build_code_enum_packer(cls)
    if is_NamedTuple(cls):
        return build_named_tuple_packer(cls, type_to_packer_resolver)
    raise KeyError(cls)


TIMED_CAKE_PACKER = type_to_packer_resolver(TimedCake)


@total_ordering
class BlockStream:
    """
    >>> bs = BlockStream(blocks=[NULL_CAKE, NULL_CAKE])
    >>> len(bytes(bs))
    67
    >>> bs == BlockStream(bytes(bs))
    True
    >>> bs != BlockStream(bytes(bs))
    False
    """

    type: CakeType
    blocks: List[Cake]

    def __init__(
        self,
        s: Optional[bytes] = None,
        blocks: Optional[Iterable[Cake]] = None,
        type: CakeType = CakeTypes.NO_CLASS,
    ):
        if s is not None:
            assert blocks is None
            len_of_s = len(s)
            assert len_of_s % SIZEOF_CAKE == 1
            self.type = CakeTypes[ord(s[:1])]
            self.blocks = []
            offset = 1
            for _ in range(len_of_s // SIZEOF_CAKE):
                end = offset + SIZEOF_CAKE
                self.blocks.append(Cake(s[offset:end]))
                offset = end
        else:
            assert blocks is not None
            self.type = type
            self.blocks = list(blocks)

    def __eq__(self, other):
        return bytes(self) == bytes(other)

    def __le__(self, other):
        return bytes(self) < bytes(other)

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

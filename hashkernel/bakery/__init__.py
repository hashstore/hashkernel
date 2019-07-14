#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import enum
import logging
import threading
from contextlib import contextmanager
from io import BytesIO
from typing import (
    IO,
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)

from hashkernel import EnsureIt, GlobalRef, Primitive, Stringable
from hashkernel.base_x import base_x
from hashkernel.guid import RANDOM_PART_SIZE, new_guid_data
from hashkernel.hashing import Hasher
from hashkernel.packer import FixedSizePacker, Packer, ProxyPacker
from hashkernel.smattr import BytesWrap, JsonWrap, SmAttr

log = logging.getLogger(__name__)


class CakeProperties(enum.Enum):
    IS_HASH = enum.auto()
    IS_FOLDER = enum.auto()
    IS_GUID = enum.auto()
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


class CakeHeader:
    modifiers: Set[CakeProperties]
    gref: Optional[GlobalRef]
    idx: Optional[int]
    name: Optional[str]
    headers: Optional["CakeHeaders"]

    def __init__(self, modifiers, gref=None, idx=None, name=None, headers=None):
        self.modifiers = modifiers
        self.gref = gref
        self.idx = idx
        self.name = name
        self.headers = headers

    def update_gref(self, gref: Union[type, GlobalRef]):
        gref = GlobalRef.ensure_it(gref)
        if self.gref is None:
            self.gref = gref
        else:
            assert self.gref == gref, f"conflict gref: {self.gref} vs {gref}"
        if self.headers is not None:
            self.headers.__types__ = None

    def __str__(self):
        return B62.alphabet[self.idx]

    def __bytes__(self):
        return bytes((self.idx,))


class _AutoRegister(type):
    def __init__(cls, name, bases, dct):
        cls.__headers__ = [None for _ in range(62)]
        cls.__by_name__: Dict[str, "CakeHeader"] = {}
        cls.__types__: Optional[Dict[GlobalRef, "CakeHeader"]] = None
        idx = 0
        for k in dct:
            if k[:1] != "_":
                if isinstance(dct[k], CakeHeader):
                    ch: CakeHeader = dct[k]
                    ch.idx = idx
                    ch.name = k
                    assert k not in cls.__by_name__
                    cls.__headers__[idx] = ch
                    cls.__by_name__[k] = ch
                    idx += 1

    def __getitem__(cls, k):
        if isinstance(k, str):
            if len(k) == 1:
                k = B62.index[k]
            else:
                return cls.__by_name__[k]
        if isinstance(k, int):
            v = cls.__headers__[k]
            if v is not None:
                return v
        raise KeyError(k)

    def register(cls, ch: CakeHeader):
        assert ch.name is not None
        assert ch.name not in cls.__by_name__
        if ch.idx is not None:
            assert cls.__headers__[ch.idx] is None
        else:
            ch.idx = next(i for i, h in enumerate(cls.__headers__) if h is None)
        cls.__headers__[ch.idx] = ch
        cls.__by_name__[ch.name] = ch

    def by_type(cls, gref: Union[type, GlobalRef]) -> Optional["CakeHeader"]:
        gref = GlobalRef.ensure_it(gref)
        if cls.__types__ is None:
            cls.__types__ = {
                h.gref: h
                for h in cls.headers()
                if CakeProperties.IS_HASH in h.modifiers and h.gref is not None
            }
        return cls.__types__[GlobalRef.ensure_it(gref)]

    def headers(cls) -> Iterable["CakeHeader"]:
        return (h for h in cls.__headers__ if h is not None)


class CakeHeaders(metaclass=_AutoRegister):
    NO_CLASS = CakeHeader({CakeProperties.IS_HASH})
    JOURNAL = CakeHeader({CakeProperties.IS_GUID, CakeProperties.IS_JOURNAL})
    FOLDER = CakeHeader({CakeProperties.IS_HASH, CakeProperties.IS_FOLDER})
    TIMESTAMP = CakeHeader({CakeProperties.IS_GUID})
    CASK = CakeHeader({CakeProperties.IS_GUID})
    BLOCKSTREAM = CakeHeader({CakeProperties.IS_HASH})
    QUESTION_MSG = CakeHeader({CakeProperties.IS_HASH})
    RESPONSE_MSG = CakeHeader({CakeProperties.IS_HASH})
    DATA_CHUNK_MSG = CakeHeader({CakeProperties.IS_HASH})
    JSON_WRAP = CakeHeader({CakeProperties.IS_HASH}, gref=GlobalRef(JsonWrap))
    BYTES_WRAP = CakeHeader({CakeProperties.IS_HASH}, gref=GlobalRef(BytesWrap))
    JOURNAL_FOLDER = CakeHeader(
        {CakeProperties.IS_GUID, CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL}
    )
    VTREE_FOLDER = CakeHeader(
        {CakeProperties.IS_GUID, CakeProperties.IS_FOLDER, CakeProperties.IS_VTREE}
    )
    MOUNT_FOLDER = CakeHeader(
        {CakeProperties.IS_GUID, CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL}
    )
    SESSION = CakeHeader({CakeProperties.IS_GUID})
    NODE = CakeHeader(
        {CakeProperties.IS_GUID, CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL}
    )
    USER = CakeHeader(
        {CakeProperties.IS_GUID, CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL}
    )


SIZEOF_CAKE = Hasher.SIZEOF + 1


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
    >>> longer_content = b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'
    >>> longer_k = Cake.from_bytes(longer_content)
    >>> str(longer_k)
    'zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT0'
    >>> len(longer_k.digest)
    32
    >>> len(set([hash(longer_k) , hash(longer_k)]))
    1

    Global Unique ID can be generated, it is 32 byte
    random sequence packed in same way.

    >>> guid = Cake.new_guid()
    >>> guid.is_guid
    True
    >>> len(str(guid))
    44
    >>> CakeProperties.typings()
    is_hash:bool
    is_folder:bool
    is_guid:bool
    is_journal:bool
    is_vtree:bool

    """

    is_hash: bool
    is_folder: bool
    is_guid: bool
    is_journal: bool
    is_vtree: bool

    __packer__: ClassVar[Packer]

    def __init__(
        self,
        s: Union[str, bytes, None],
        digest: Optional[bytes] = None,
        header: Optional[CakeHeader] = None,
    ) -> None:
        if s is None:
            assert (
                digest is not None and header is not None
            ), f"both digest={digest} and header={header} required"
            self.digest = digest
            self.header = header
        elif isinstance(s, bytes):
            assert len(s) == SIZEOF_CAKE, f"invalid length of s: {len(s)}"
            self.digest = s[:-1]
            self.header = CakeHeaders[ord(s[-1:])]
        else:
            self.digest = B62.decode(s[:-1])
            self.header = CakeHeaders[s[-1:]]
        CakeProperties.set_properties(self, *self.header.modifiers)
        assert (
            len(self.digest) == Hasher.SIZEOF
        ), f"invalid cake digest: {s} {digest} {header} "

    def uniform_digest(self):
        """
        in case of guid first bytes of digest contains time and ttl which
        is not unifomrly distributed

        :return: Portion of digest that could be used for sharding and routing
        """
        return self.digest[32 - RANDOM_PART_SIZE :]

    @staticmethod
    def from_stream(fd: IO[bytes], header=CakeHeaders.NO_CLASS) -> "Cake":
        assert CakeProperties.IS_HASH in header.modifiers
        return Cake(
            None, digest=Hasher().update_from_stream(fd).digest(), header=header
        )

    @staticmethod
    def from_bytes(s: bytes, header=CakeHeaders.NO_CLASS) -> "Cake":
        return Cake.from_stream(BytesIO(s), header)

    @staticmethod
    def from_file(file: str, header) -> "Cake":
        return Cake.from_stream(open(file, "rb"), header)

    @staticmethod
    def new_guid(header=CakeHeaders.TIMESTAMP) -> "Cake":
        return Cake(None, digest=new_guid_data(), header=header)

    @staticmethod
    def from_digest36(digest: str, header: CakeHeader):
        return Cake(None, B36.decode(digest), header)

    def assert_guid(self) -> None:
        assert self.is_guid, f"has to be a guid: {self}"

    def __str__(self) -> str:
        return B62.encode(self.digest) + str(self.header)

    def digest36(self) -> str:
        return B36.encode(self.digest)

    def __repr__(self) -> str:
        return f"Cake({str(self)!r})"

    def __hash__(self) -> int:
        if not (hasattr(self, "_hash")):
            self._hash = hash(self.digest)
        return self._hash

    def __eq__(self, other) -> bool:
        if not isinstance(other, Cake):
            return False
        return self.digest == other.digest and self.header == other.header

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __bytes__(self):
        return self.digest + bytes(self.header)


Cake.__packer__ = ProxyPacker(Cake, FixedSizePacker(SIZEOF_CAKE))


class HasCake(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def cake(self) -> Cake:
        raise NotImplementedError("subclasses must override")


class HasCakeFromBytes:
    def cake(self) -> Cake:
        return Cake.from_bytes(
            bytes(self),  # type:ignore
            header=CakeHeaders.by_type(self.__class__),
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


class BlockStream:

    blocks: List[Cake]

    def __init__(self, s: bytes = b""):
        len_of_s = len(s)
        assert SIZEOF_CAKE % len_of_s == 0
        self.blocks = []
        offset = 0
        for i in range(1, 1 + len_of_s // SIZEOF_CAKE):
            end = i * SIZEOF_CAKE
            self.blocks.append(Cake(s[offset:end]))
            offset = end

    def __bytes__(self):
        return b"".join(map(bytes, self.blocks))


CakeHeaders.BLOCKSTREAM.update_gref(BlockStream)
CakeHeaders.QUESTION_MSG.update_gref(QuestionMsg)
CakeHeaders.RESPONSE_MSG.update_gref(ResponseMsg)
CakeHeaders.DATA_CHUNK_MSG.update_gref(DataChunkMsg)


class HashSession(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def load_content(self, cake: Cake) -> bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    async def store_content(
        self, cake_header: CakeHeader, content: Union[bytes, IO[bytes]]
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

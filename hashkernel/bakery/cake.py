#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
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
    Tuple,
    Union,
)

from hashkernel import EnsureIt, GlobalRef, Primitive, Stringable
from hashkernel.bakery import (
    CakeMode,
    CakeProperties,
    CakeType,
    CakeTypes,
    TypesProcessor,
)
from hashkernel.base_x import base_x
from hashkernel.guid import new_guid_data
from hashkernel.hashing import Hasher, shard_based_on_two_bites, shard_name_int
from hashkernel.plugins import query_plugins
from hashkernel.smattr import SmAttr

log = logging.getLogger(__name__)

B62 = base_x(62)


class CakeHeader:
    """
    >>> CakeHeader.typings()
    NO_CLASS:ClassVar['CakeHeader']
    JOURNAL:ClassVar['CakeHeader']
    FOLDER:ClassVar['CakeHeader']
    TIMESTAMP:ClassVar['CakeHeader']
    QUESTION_MSG:ClassVar['CakeHeader']
    RESPONSE_MSG:ClassVar['CakeHeader']
    DATA_CHUNK_MSG:ClassVar['CakeHeader']
    JSON_WRAP:ClassVar['CakeHeader']
    BYTES_WRAP:ClassVar['CakeHeader']
    JOURNAL_FOLDER:ClassVar['CakeHeader']
    VTREE_FOLDER:ClassVar['CakeHeader']
    MOUNT_FOLDER:ClassVar['CakeHeader']
    SESSION:ClassVar['CakeHeader']
    NODE:ClassVar['CakeHeader']
    USER:ClassVar['CakeHeader']
    """

    NO_CLASS: ClassVar["CakeHeader"]
    JOURNAL: ClassVar["CakeHeader"]
    FOLDER: ClassVar["CakeHeader"]
    TIMESTAMP: ClassVar["CakeHeader"]
    QUESTION_MSG: ClassVar["CakeHeader"]
    RESPONSE_MSG: ClassVar["CakeHeader"]
    DATA_CHUNK_MSG: ClassVar["CakeHeader"]
    JSON_WRAP: ClassVar["CakeHeader"]
    BYTES_WRAP: ClassVar["CakeHeader"]
    JOURNAL_FOLDER: ClassVar["CakeHeader"]
    VTREE_FOLDER: ClassVar["CakeHeader"]
    MOUNT_FOLDER: ClassVar["CakeHeader"]
    SESSION: ClassVar["CakeHeader"]
    NODE: ClassVar["CakeHeader"]
    USER: ClassVar["CakeHeader"]

    __magic__ = 5321
    __headers__: List[Optional["CakeHeader"]] = [None for _ in range(62)]

    idx: int
    mode: CakeMode
    name: str
    modifiers: Set[CakeProperties]
    gref: Optional[GlobalRef]

    def __new__(cls, value):
        if type(value) is cls:
            return value
        if isinstance(value, str):
            value = B62.index[value]
        if isinstance(value, int):
            return cls.__headers__[value]
        assert isinstance(value, tuple) and value[0] == cls.__magic__
        _, idx, name, mode, ct = value
        inst = cls.__headers__[idx]
        if inst is None:
            inst = super(CakeHeader, cls).__new__(cls)
            inst.name = name
            inst.idx = idx
            inst.modifiers = set(mode.modifiers)
            inst.mode = mode
            inst.gref = None
            setattr(cls, name, inst)
            cls.__headers__[idx] = inst
        inst.modifiers.update(ct.modifiers)
        if ct.gref is not None:
            if inst.gref is None:
                inst.gref = ct.gref
            else:
                assert inst.gref == ct.gref, f"conflict gref: {inst.gref} vs {ct.gref}"
        assert inst.mode == mode, f"conflict mode: {inst.mode.name} vs {mode.name}"
        return inst

    def __str__(self):
        return B62.alphabet[self.idx]

    @classmethod
    def typings(cls):
        for h in cls.headers():
            print(f"{h.name}:ClassVar['{type(h).__name__}']")

    @classmethod
    def headers(cls) -> Iterable["CakeHeader"]:
        return (h for h in cls.__headers__ if h is not None)

    @classmethod
    def guess_from_type(cls, gref: Union[type, GlobalRef]) -> "CakeHeader":
        gref = GlobalRef.ensure_it(gref)
        for h in sorted(cls.headers(), lambda h: h.mode == CakeMode.GUID):
            if gref == h.gref:
                return h
        return CakeHeader.NO_CLASS

    def __bytes__(self):
        return bytes((self.idx,))

    @staticmethod
    def init_headers(*plugins):
        for types in plugins:
            headers = types.__headers__
            for i in range(len(headers)):
                idx = types.__start_index__ + i
                CakeHeader((CakeHeader.__magic__, idx, *headers[i]))


CakeHeader.init_headers(
    CakeTypes, *query_plugins(TypesProcessor, "hashkernel.cake_types")
)


MAX_NUM_OF_SHARDS = 8192
SIZEOF_CAKE = Hasher.SIZEOF + 1


class Cake(Stringable, EnsureIt, Primitive):
    """
    Stands for Content Address Key.

    Content addressing scheme using SHA256. Base62 encoding is
    used to encode bytes.

    >>> list(CakeMode) #doctest: +NORMALIZE_WHITESPACE
    [<CakeMode.SHA256: 0>, <CakeMode.GUID: 1>]

    >>> short_content = b'The quick brown fox jumps over'
    >>> short_k = Cake.from_bytes(short_content)
    >>> short_k.header.mode
    <CakeMode.SHA256: 0>
    >>> str(short_k)
    'l01natqrQGg1ueJkFIc9mUYt18gcJjdsPLSLyzGgjY70'

    >>> longer_content = b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'
    >>> longer_k = Cake.from_bytes(longer_content)
    >>> longer_k.header.mode
    <CakeMode.SHA256: 0>
    >>> str(longer_k)
    'zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT0'
    >>> len(longer_k.digest)
    32
    >>> len(set([hash(longer_k) , hash(longer_k)]))
    1

    Global Unique ID can be generated, it is 32 byte
    random sequence packed in same way.

    >>> guid = Cake.new_guid()
    >>> guid.header.mode
    <CakeMode.GUID: 1>
    >>> len(str(guid))
    44
    >>> CakeProperties.typings()
    is_immutable:bool
    is_folder:bool
    is_guid:bool
    is_journal:bool
    is_vtree:bool

    """

    is_immutable: bool
    is_folder: bool
    is_guid: bool
    is_journal: bool
    is_vtree: bool

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
            self.header = CakeHeader(ord(s[-1:]))
        else:
            self.digest = B62.decode(s[:-1])
            self.header = CakeHeader(s[-1:])
        CakeProperties.set_properties(self, *self.header.modifiers)
        assert (
            len(self.digest) == Hasher.SIZEOF
        ), f"invalid cake digest: {s} {digest} {header} "

    def shard_num(self, base: int) -> int:
        """
        >>> Cake.from_bytes(b'').shard_num(8192)
        6907
        >>> Cake.from_bytes(b' ').shard_num(8192)
        6551
        >>> Cake('zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT0').shard_num(8192)
        2278

        """
        return shard_based_on_two_bites(self.uniform_digest(), base)

    def uniform_digest(self):
        """
        in case of guid first eight bytes of digest contains time which
        is not unifomrly distributed

        :return: Portion of digest that could be used for sharding and routing
        """
        return self.digest[8:]

    def shard_name(self, base: int) -> str:
        return shard_name_int(self.shard_num(base))

    @staticmethod
    def from_stream(fd: IO[bytes], header=CakeHeader.NO_CLASS) -> "Cake":
        assert CakeProperties.IS_IMMUTABLE in header.modifiers
        return Cake(
            None, digest=Hasher().update_from_stream(fd).digest(), header=header
        )

    @staticmethod
    def from_bytes(s: bytes, header=CakeHeader.NO_CLASS) -> "Cake":
        return Cake.from_stream(BytesIO(s), header)

    @staticmethod
    def from_file(file: str, header) -> "Cake":
        return Cake.from_stream(open(file, "rb"), header)

    @staticmethod
    def new_guid(header=CakeHeader.TIMESTAMP) -> "Cake":
        return Cake(None, digest=new_guid_data(), header=header)

    def assert_guid(self) -> None:
        assert self.is_guid, f"has to be a guid: {self}"

    def __str__(self) -> str:
        return B62.encode(self.digest) + str(self.header)

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


class HasCake(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def cake(self) -> Cake:
        raise NotImplementedError("subclasses must override")


class HasCakeFromBytes:
    def cake(self) -> Cake:
        return Cake.from_bytes(
            bytes(self),  # type:ignore
            header=CakeHeader.guess_from_type(type(self)),
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


class MsgTypes(metaclass=TypesProcessor):
    __start_index__ = CakeHeader.QUESTION_MSG.idx
    QUESTION_MSG = CakeType(mode=CakeMode.SHA256, gref=GlobalRef(QuestionMsg))
    RESPONSE_MSG = CakeType(mode=CakeMode.SHA256, gref=GlobalRef(ResponseMsg))
    DATA_CHUNK_MSG = CakeType(mode=CakeMode.SHA256, gref=GlobalRef(DataChunkMsg))


CakeHeader.init_headers(MsgTypes, MsgTypes)


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

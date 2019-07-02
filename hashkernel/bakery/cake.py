#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import enum
import json
import logging
import os
import threading
from contextlib import contextmanager
from datetime import datetime
from io import BytesIO
from pathlib import PurePosixPath
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

import pkg_resources

from hashkernel import (
    EnsureIt,
    GlobalRef,
    Jsonable,
    Primitive,
    Stringable,
    utf8_decode,
    utf8_encode,
)
from hashkernel.bakery import CakeMode, CakeProperties, CakeTypes, TypesProcessor
from hashkernel.base_x import base_x
from hashkernel.file_types import file_types, guess_name
from hashkernel.guid import new_guid_data
from hashkernel.hashing import (
    HashBytes,
    Hasher,
    shard_based_on_two_bites,
    shard_name_int,
)
from hashkernel.smattr import BytesWrap, JsonWrap, SmAttr

log = logging.getLogger(__name__)

B62 = base_x(62)


def _cake_types_plugins():
    return filter(
        lambda v: issubclass(type(v), TypesProcessor),
        (ep.load() for ep in pkg_resources.iter_entry_points("hashkernel.cake_types")),
    )


class CakeHeader:
    """
    >>> CakeHeader.typings()
    NO_CLASS_INLINE:ClassVar['CakeHeader']
    NO_CLASS:ClassVar['CakeHeader']
    JOURNAL:ClassVar['CakeHeader']
    FOLDER_INLINE:ClassVar['CakeHeader']
    FOLDER:ClassVar['CakeHeader']
    TIMESTAMP:ClassVar['CakeHeader']
    QUESTION_MSG_INLINE:ClassVar['CakeHeader']
    QUESTION_MSG:ClassVar['CakeHeader']
    RESPONSE_MSG_INLINE:ClassVar['CakeHeader']
    RESPONSE_MSG:ClassVar['CakeHeader']
    DATA_CHUNK_MSG_INLINE:ClassVar['CakeHeader']
    DATA_CHUNK_MSG:ClassVar['CakeHeader']
    JSON_WRAP_INLINE:ClassVar['CakeHeader']
    JSON_WRAP:ClassVar['CakeHeader']
    BYTES_WRAP_INLINE:ClassVar['CakeHeader']
    BYTES_WRAP:ClassVar['CakeHeader']
    JOURNAL_FOLDER:ClassVar['CakeHeader']
    VTREE_FOLDER:ClassVar['CakeHeader']
    MOUNT_FOLDER:ClassVar['CakeHeader']
    SESSION:ClassVar['CakeHeader']
    NODE:ClassVar['CakeHeader']
    USER:ClassVar['CakeHeader']
    """

    NO_CLASS_INLINE: ClassVar["CakeHeader"]
    NO_CLASS: ClassVar["CakeHeader"]
    JOURNAL: ClassVar["CakeHeader"]
    FOLDER_INLINE: ClassVar["CakeHeader"]
    FOLDER: ClassVar["CakeHeader"]
    TIMESTAMP: ClassVar["CakeHeader"]
    QUESTION_MSG_INLINE: ClassVar["CakeHeader"]
    QUESTION_MSG: ClassVar["CakeHeader"]
    RESPONSE_MSG_INLINE: ClassVar["CakeHeader"]
    RESPONSE_MSG: ClassVar["CakeHeader"]
    DATA_CHUNK_MSG_INLINE: ClassVar["CakeHeader"]
    DATA_CHUNK_MSG: ClassVar["CakeHeader"]
    JSON_WRAP_INLINE: ClassVar["CakeHeader"]
    JSON_WRAP: ClassVar["CakeHeader"]
    BYTES_WRAP_INLINE: ClassVar["CakeHeader"]
    BYTES_WRAP: ClassVar["CakeHeader"]
    JOURNAL_FOLDER: ClassVar["CakeHeader"]
    VTREE_FOLDER: ClassVar["CakeHeader"]
    MOUNT_FOLDER: ClassVar["CakeHeader"]
    SESSION: ClassVar["CakeHeader"]
    NODE: ClassVar["CakeHeader"]
    USER: ClassVar["CakeHeader"]

    __magic__ = 5321
    __headers__: List[Union["CakeHeader", None]] = [None for _ in range(62)]

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
        if isinstance(value, tuple) and value[0] == cls.__magic__:
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
                elif inst.gref != ct.gref:
                    raise ValueError(f"conflict gref: {inst.gref} vs {ct.gref}")
            if inst.mode != mode:
                raise ValueError(f"conflict mode: {inst.mode.name} vs {mode.name}")
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
    def inline(cls, header: "CakeHeader"):
        if header.mode != CakeMode.SHA256:
            raise AssertionError(f"has to be {CakeMode.SHA256}")
        return cls(header.idx - 1)

    @classmethod
    def guess_from_type(cls, gref: Union[type, GlobalRef]) -> "CakeHeader":
        gref = GlobalRef.ensure_it(gref)
        for h in cls.headers():
            if CakeProperties.IS_RESOLVED in h.modifiers:
                if gref == h.gref:
                    return h
        return CakeHeader.NO_CLASS


for types in (CakeTypes, *_cake_types_plugins()):
    headers = types.__headers__
    for i in range(len(headers)):
        idx = types.__start_index__ + i
        CakeHeader((CakeHeader.__magic__, idx, *headers[i]))


MAX_NUM_OF_SHARDS = 8192


inline_max_bytes = 32


def nop_on_chunk(chunk: bytes) -> None:
    """
    Does noting

    >>> nop_on_chunk(b'')

    :param read_buffer: takes bytes
    :return: does nothing
    """
    pass


def process_stream(
    fd: IO[bytes],
    on_chunk: Callable[[bytes], None] = nop_on_chunk,
    chunk_size: int = 65355,
) -> Tuple[bytes, Optional[bytes]]:
    """
    process stream to calculate hash, length of data,
    and if it is smaller then hash size, holds on to stream
    content to use it instead of hash.

    :param fd: stream
    :param on_chunk: function  called on every chunk
    :return: (<hash>, Optional[<inline_data>])
    """
    inline_data = bytes()
    hasher = Hasher()
    length = 0
    while True:
        chunk = fd.read(chunk_size)
        if len(chunk) <= 0:
            break
        length += len(chunk)
        hasher.update(chunk)
        on_chunk(chunk)
        if length <= inline_max_bytes:
            inline_data += chunk
    fd.close()
    return (hasher.digest(), None if length > inline_max_bytes else inline_data)


class Cake(Stringable, EnsureIt, Primitive):
    """
    Stands for Content Address Key.

    Content addressing scheme using SHA256. For small
    content ( <=32 bytes) data is embeded  in key.  Header byte is
    followed by hash digest or inlined data. header byte split in two
    halves: `CakeType` and `CakeRole`. Base62 encoding is
    used to encode bytes.

    >>> list(CakeMode) #doctest: +NORMALIZE_WHITESPACE
    [<CakeMode.INLINE: 0>, <CakeMode.SHA256: 1>,
    <CakeMode.GUID: 2>]

    >>> short_content = b'The quick brown fox jumps over'
    >>> short_k = Cake.from_bytes(short_content)
    >>> short_k.header.mode
    <CakeMode.INLINE: 0>
    >>> short_k.is_inlined
    True
    >>> short_k.data() is not None
    True
    >>> short_k.data() == short_content
    True
    >>> str(short_k)
    '1aMUQDApalaaYbXFjBVMMvyCAMfSPcTojI0745igi0'

    Longer content is hashed with SHA256:

    >>> longer_content = b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'
    >>> longer_k = Cake.from_bytes(longer_content)
    >>> longer_k.header.mode
    <CakeMode.SHA256: 1>
    >>> longer_k.is_inlined
    False
    >>> longer_k.data() is None
    True
    >>> str(longer_k)
    'zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT1'
    >>> len(longer_k.hash_bytes())
    32
    >>> len(longer_k.digest())
    32
    >>> len(set([hash(longer_k) , hash(longer_k)]))
    1

    Global Unique ID can be generated, it is 32 byte
    random sequence packed in same way.

    >>> guid = Cake.new_guid()
    >>> guid.header.mode
    <CakeMode.GUID: 2>
    >>> len(str(guid))
    44
    >>> CakeProperties.typings()
    is_inlined:bool
    is_immutable:bool
    is_resolved:bool
    is_folder:bool
    is_guid:bool
    is_journal:bool
    is_vtree:bool

    """

    is_inlined: bool
    is_immutable: bool
    is_resolved: bool
    is_folder: bool
    is_guid: bool
    is_journal: bool
    is_vtree: bool

    def __init__(
        self,
        s: Optional[str],
        data: Optional[bytes] = None,
        header: Optional[CakeHeader] = None,
    ) -> None:
        if s is None:
            if data is None or header is None:
                raise AssertionError(f"both data={data} and header={header} required")
            self._data = data
            self.header = header
        else:
            self._data = B62.decode(s[:-1])
            self.header = CakeHeader(s[-1:])
        CakeProperties.set_properties(self, *self.header.modifiers)
        if not self.is_inlined and len(self._data) != 32:
            raise AssertionError(
                f"invalid CAKey: {s} {data} {header} {self.is_inlined}"
            )

    def shard_num(self, base: int) -> int:
        """
        >>> Cake('0').shard_num(8192)
        6907
        >>> Cake.from_bytes(b' ').shard_num(8192)
        6551
        >>> Cake('zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT1').shard_num(8192)
        2278

        """
        return shard_based_on_two_bites(self.uniform_digest(), base)

    def uniform_digest(self):
        """
        in case of guid first eight bytes of digest contains time which
        is not unifomrly distributed

        :return: Protion of digest that could be used for sharding and routing
        """
        return self.digest()[8:]

    def shard_name(self, base: int) -> str:
        return shard_name_int(self.shard_num(base))

    @staticmethod
    def from_digest_and_inline_data(
        digest: bytes, buffer: Optional[bytes], header=CakeHeader.NO_CLASS
    ) -> "Cake":
        if buffer is not None and len(buffer) <= inline_max_bytes:
            return Cake(None, data=buffer, header=CakeHeader.inline(header))
        else:
            return Cake(None, data=digest, header=header)

    @staticmethod
    def from_stream(fd: IO[bytes], header=CakeHeader.NO_CLASS) -> "Cake":
        digest, inline_data = process_stream(fd)
        return Cake.from_digest_and_inline_data(digest, inline_data, header)

    @staticmethod
    def from_bytes(s: bytes, header=CakeHeader.NO_CLASS) -> "Cake":
        return Cake.from_stream(BytesIO(s), header)

    @staticmethod
    def from_file(file: str, header) -> "Cake":
        return Cake.from_stream(open(file, "rb"), header)

    @staticmethod
    def new_guid(header=CakeHeader.TIMESTAMP) -> "Cake":
        cake = Cake.random_cake(header)
        cake.assert_guid()
        return cake

    @staticmethod
    def random_cake(header):
        return Cake(None, data=new_guid_data(), header=header)

    def augment_header(self, new_header: CakeHeader) -> "Cake":
        self.assert_guid()
        if new_header == self.header:
            return self
        return Cake(None, data=self._data, header=new_header)

    def has_data(self) -> bool:
        return self.is_inlined

    def data(self) -> Optional[bytes]:
        return self._data if self.is_inlined else None

    def digest(self) -> bytes:
        if not (hasattr(self, "_digest")):
            if self.is_inlined:
                self._digest = Hasher(self._data).digest()
            else:
                self._digest = self._data
        return self._digest

    def assert_guid(self) -> None:
        if not self.is_guid:
            raise AssertionError("has to be a guid: %r" % self)

    def hash_bytes(self) -> bytes:
        """
        :raise AssertionError when Cake is not hash based
        :return: hash in bytes
        """
        if not self.is_resolved:
            raise AssertionError(f"Not-hash {self.header.mode} {self}")
        return self._data

    def __str__(self) -> str:
        return B62.encode(self._data) + str(self.header)

    def __repr__(self) -> str:
        return f"Cake({str(self)!r})"

    def __hash__(self) -> int:
        if not (hasattr(self, "_hash")):
            self._hash = hash(self.digest())
        return self._hash

    def __eq__(self, other) -> bool:
        if not isinstance(other, Cake):
            return False
        return self._data == other._data and self.header == other.header

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)


HashBytes.register(Cake)


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


class PatchAction(Jsonable, enum.Enum):
    update = +1
    delete = -1

    @classmethod
    def __factory__(cls):
        return lambda s: cls[s]

    def __str__(self):
        return self.name

    def __to_json__(self):
        return str(self)


class RackRow(SmAttr):
    name: str
    cake: Optional[Cake]


class CakeRack(Jsonable):
    """
    sorted dictionary of names and corresponding Cakes

    >>> short_k = Cake.from_bytes(b'The quick brown fox jumps over')
    >>> longer_k = Cake.from_bytes(b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.')

    >>> cakes = CakeRack()
    >>> cakes['short'] = short_k
    >>> cakes['longer'] = longer_k
    >>> len(cakes)
    2

    >>> cakes.keys()
    ['longer', 'short']
    >>> str(cakes.cake())
    's6dPCvdSRfton1gG9OZnzRVe5m3Z0u3ixl3tU4xGLlr4'
    >>> cakes.size()
    117
    >>> cakes.content()
    '[["longer", "short"], ["zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT1", "1aMUQDApalaaYbXFjBVMMvyCAMfSPcTojI0745igi0"]]'
    >>> cakes.get_name_by_cake("zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT1")
    'longer'
    """

    def __init__(self, o: Any = None) -> None:
        self.store: Dict[str, Optional[Cake]] = {}
        self._clear_cached()
        if o is not None:
            self.parse(o)

    def _clear_cached(self):
        self._inverse: Any = None
        self._cake: Any = None
        self._content: Any = None
        self._size: Any = None
        self._in_bytes: Any = None
        self._defined: Any = None

    def inverse(self) -> Dict[Optional[Cake], str]:
        if self._inverse is None:
            self._inverse = {v: k for k, v in self.store.items()}
        return self._inverse

    def cake(self) -> Cake:
        if self._cake is None:
            in_bytes = bytes(self)
            self._cake = Cake.from_digest_and_inline_data(
                Hasher(in_bytes).digest(), in_bytes, header=CakeHeader.FOLDER
            )
        return self._cake

    def content(self) -> str:
        if self._content is None:
            self._content = str(self)
        return self._content

    def __bytes__(self) -> bytes:
        if self._in_bytes is None:
            self._in_bytes = utf8_encode(self.content())
        return self._in_bytes

    def size(self) -> int:
        if self._size is None:
            self._size = len(bytes(self))
        return self._size

    def is_defined(self) -> bool:
        if self._defined is None:
            self._defined = all(v is not None for v in self.store.values())
        return self._defined

    def parse(self, o: Any) -> "CakeRack":
        self._clear_cached()
        if isinstance(o, bytes):
            names, cakes = json.loads(utf8_decode(o))
        elif isinstance(o, str):
            names, cakes = json.loads(o)
        elif type(o) in [list, tuple] and len(o) == 2:
            names, cakes = o
        else:
            names, cakes = json.load(o)
        self.store.update(zip(names, map(Cake.ensure_it_or_none, cakes)))
        return self

    def merge(
        self, previous: "CakeRack"
    ) -> Iterable[Tuple[PatchAction, str, Optional[Cake]]]:
        """
        >>> o1 = Cake.from_bytes(b'The quick brown fox jumps over')
        >>> o2v1 = Cake.from_bytes(b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.')
        >>> o2v2 = Cake.from_bytes(b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. v2')
        >>> o3 = CakeRack().cake()
        >>> r1 = CakeRack()
        >>> r1['o1']=o1
        >>> r1['o2']=o2v1
        >>> r1['o3']=None
        >>> r2 = CakeRack()
        >>> r2['o1']=o1
        >>> r2['o2']=o2v2
        >>> r2['o3']=o3
        >>> list(r2.merge(r1))
        [(<PatchAction.update: 1>, 'o2', Cake('NlXF0MZtHOZ3EE0Z2zPz80I9YG7vbN7KAbm1qJv3EZ51'))]
        >>> list(r1.merge(r2))
        [(<PatchAction.update: 1>, 'o2', Cake('zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT1'))]
        >>> r1['o1'] = None
        >>> list(r2.merge(r1)) #doctest: +NORMALIZE_WHITESPACE
        [(<PatchAction.delete: -1>, 'o1', None),
        (<PatchAction.update: 1>, 'o1', Cake('1aMUQDApalaaYbXFjBVMMvyCAMfSPcTojI0745igi0')),
        (<PatchAction.update: 1>, 'o2', Cake('NlXF0MZtHOZ3EE0Z2zPz80I9YG7vbN7KAbm1qJv3EZ51'))]
        >>> list(r1.merge(r2)) #doctest: +NORMALIZE_WHITESPACE
        [(<PatchAction.delete: -1>, 'o1', None),
        (<PatchAction.update: 1>, 'o1', None),
        (<PatchAction.update: 1>, 'o2', Cake('zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT1'))]
        >>> del r1["o2"]
        >>> list(r2.merge(r1)) #doctest: +NORMALIZE_WHITESPACE
        [(<PatchAction.delete: -1>, 'o1', None),
        (<PatchAction.update: 1>, 'o1', Cake('1aMUQDApalaaYbXFjBVMMvyCAMfSPcTojI0745igi0')),
        (<PatchAction.update: 1>, 'o2', Cake('NlXF0MZtHOZ3EE0Z2zPz80I9YG7vbN7KAbm1qJv3EZ51'))]
        >>> list(r1.merge(r2)) #doctest: +NORMALIZE_WHITESPACE
        [(<PatchAction.delete: -1>, 'o1', None),
        (<PatchAction.update: 1>, 'o1', None),
        (<PatchAction.delete: -1>, 'o2', None)]
        """
        for k in sorted(list(set(self.keys() + previous.keys()))):
            if k not in self and k in previous:
                yield PatchAction.delete, k, None
            else:
                v = self[k]
                neuron = self.is_neuron(k)
                if k in self and k not in previous:
                    yield PatchAction.update, k, v
                else:
                    prev_v = previous[k]
                    prev_neuron = previous.is_neuron(k)
                    if v != prev_v:
                        if neuron == True and prev_neuron == True:
                            continue
                        if prev_neuron == neuron:
                            yield PatchAction.update, k, v
                        else:
                            yield PatchAction.delete, k, None
                            yield PatchAction.update, k, v

    def is_neuron(self, k) -> Optional[bool]:
        v = self.store[k]
        return v is None or v.is_folder

    def __iter__(self) -> Iterable[str]:
        return iter(self.keys())

    def __setitem__(self, k: str, v: Union[Cake, str, None]) -> None:
        self._clear_cached()
        self.store[k] = Cake.ensure_it_or_none(v)

    def __delitem__(self, k: str):
        self._clear_cached()
        del self.store[k]

    def __getitem__(self, k: str) -> Optional[Cake]:
        return self.store[k]

    def __len__(self) -> int:
        return len(self.store)

    def __contains__(self, k: str) -> bool:
        return k in self.store

    def get_name_by_cake(self, k: Union[Cake, str]):
        return self.inverse()[Cake.ensure_it(k)]

    def keys(self) -> List[str]:
        names = list(self.store.keys())
        names.sort()
        return names

    def get_cakes(self, names=None) -> List[Optional[Cake]]:
        if names is None:
            names = self.keys()
        return [self.store[k] for k in names]

    def __to_json__(self) -> Tuple[List[str], List[Optional[Cake]]]:
        keys = self.keys()
        return (keys, self.get_cakes(keys))


HasCake.register(CakeRack)


class HashSession(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def load_content(self, cake: Cake) -> bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def store_content(
        self, cake_header: CakeHeader, content: Union[bytes, IO[bytes]]
    ) -> Cake:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def edit_journal(self, journal: Cake, content: Cake):
        raise NotImplementedError("subclasses must override")

    def close(self):
        pass


class HashContext:
    @staticmethod
    def get() -> HashSession:
        return threading.local().hash_ctx

    @staticmethod
    def set(ctx: HashSession):
        threading.local().hash_ctx = ctx

    @contextmanager
    @staticmethod
    def context(factory: Callable[[], HashSession]):
        session = factory()
        HashContext.set(session)
        try:
            yield session
        finally:
            session.close()
            HashContext.set(None)


HashContext.set(None)

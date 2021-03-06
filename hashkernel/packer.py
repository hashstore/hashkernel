import abc
import struct
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from nanotime import nanotime

from hashkernel import BitMask, utf8_decode, utf8_encode
from hashkernel.files.buffer import FileBytes
from hashkernel.typings import is_NamedTuple, is_subclass

Buffer = Union[FileBytes, bytes]


class NeedMoreBytes(Exception):
    def __init__(self, how_much: int = None):
        self.how_much = how_much

    @classmethod
    def check_buffer(cls, buff_len, fragment_end) -> int:
        if buff_len < fragment_end:
            raise cls(fragment_end - buff_len)
        return fragment_end


class Packer(metaclass=abc.ABCMeta):
    cls: type
    size: Optional[int] = None

    def fixed_size(self) -> bool:
        return self.size is not None

    @abc.abstractmethod
    def pack(self, v: Any) -> bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def unpack(self, buffer: Buffer, offset: int) -> Tuple[Any, int]:
        raise NotImplementedError("subclasses must override")

    def unpack_whole_buffer(self, buffer: Buffer) -> Any:
        obj, offset = self.unpack(buffer, 0)
        assert len(buffer) == offset
        return obj


MARK_BIT = BitMask(7)


class AdjustableSizePacker(Packer):
    """
    >>> asp3 = AdjustableSizePacker(3)
    >>> asp3.unpack(bytes([0x83]), 0)
    (3, 1)
    >>> asp3.unpack(bytes([0xff]), 0)
    (127, 1)
    >>> asp3.unpack(bytes([0x00,0x81]), 0)
    (128, 2)
    >>> asp3.unpack(bytes([0x7f,0x81]), 0)
    (255, 2)
    >>> asp3.unpack(bytes([0x00,0xfd]),0)
    (16000, 2)
    >>> asp3.unpack(bytes([0x69,0x04,0x81]),0)
    (17001, 3)
    >>> asp3.unpack(bytes([0x00,0x09,0xfa]),0)
    (2000000, 3)
    >>> asp3.unpack(bytes([0x00,0x09,0x7a,0x81]),0)
    Traceback (most recent call last):
    ...
    ValueError: No end bit

    >>> asp3.unpack(bytes([0x00,0x09]),0)
    Traceback (most recent call last):
    ...
    hashkernel.packer.NeedMoreBytes: 1
    >>> asp3.pack(3).hex()
    '83'
    >>> asp3.pack(127).hex()
    'ff'
    >>> asp3.pack(128).hex()
    '0081'
    >>> asp3.pack(255).hex()
    '7f81'
    >>> asp3.pack(16000).hex()
    '00fd'
    >>> asp3.pack(17001).hex()
    '690481'
    >>> asp3.pack(2000000).hex()
    '0009fa'
    >>> asp3.pack(3000000).hex()
    Traceback (most recent call last):
    ...
    ValueError: Size is too big: 3000000
    """

    max_size: int

    cls = int

    def __init__(self, max_size: int):
        self.max_size = max_size

    def pack(self, v: int) -> bytes:
        sz_bytes = []
        shift = v
        for _ in range(self.max_size):
            numerical = shift & MARK_BIT.inverse
            shift = shift >> MARK_BIT.position
            if 0 == shift:
                sz_bytes.append(numerical | MARK_BIT.mask)
                return bytes(sz_bytes)
            else:
                sz_bytes.append(numerical)
        raise ValueError(f"Size is too big: {v}")

    def unpack(self, buffer: Buffer, offset: int) -> Tuple[int, int]:
        """

        Returns:
            size: Unpacked size
            new_offset: new offset in buffer

        """
        sz = 0
        buff_len = len(buffer)
        for i in range(self.max_size):
            NeedMoreBytes.check_buffer(buff_len, offset + i + 1)
            v = buffer[offset + i]
            end = v & MARK_BIT.mask
            sz += (v & MARK_BIT.inverse) << (i * MARK_BIT.position)
            if end:
                return sz, offset + i + 1
        raise ValueError("No end bit")


class SizedPacker(Packer):
    cls = bytes

    def __init__(self, size_packer):
        self.size_packer = size_packer

    def pack(self, v: bytes) -> bytes:
        return self.size_packer.pack(len(v)) + v

    def unpack(self, buffer: Buffer, offset: int) -> Tuple[bytes, int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        size, data_offset = self.size_packer.unpack(buffer, offset)
        new_offset = NeedMoreBytes.check_buffer(len(buffer), data_offset + size)
        return buffer[data_offset:new_offset], new_offset


class GreedyBytesPacker(Packer):
    """
    Read buffer to the end, with assumption that buffer end is
    aligned with end of last variable
    """

    cls = bytes

    def pack(self, v: bytes) -> bytes:
        return v

    def unpack(self, buffer: Buffer, offset: int) -> Tuple[bytes, int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        new_offset = len(buffer)
        return buffer[offset:new_offset], new_offset


class FixedSizePacker(Packer):
    cls = bytes

    def __init__(self, size: int) -> None:
        self.size = size

    def pack(self, v: bytes) -> bytes:
        assert len(v) == self.size, f"{len(v)} != {self.size}"
        return v

    def unpack(self, buffer: Buffer, offset: int) -> Tuple[bytes, int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        new_offset = offset + self.size
        NeedMoreBytes.check_buffer(len(buffer), new_offset)
        return buffer[offset:new_offset], new_offset


class TypePacker(Packer):
    def __init__(self, cls: type, fmt: str) -> None:
        self.cls = cls
        self.fmt = fmt
        self.size = struct.calcsize(self.fmt)

    def pack(self, v: Any) -> bytes:
        return struct.pack(self.fmt, v)

    def unpack(self, buffer: Buffer, offset: int) -> Tuple[Any, int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        new_offset = self.size + offset
        NeedMoreBytes.check_buffer(len(buffer), new_offset)
        unpacked_values = struct.unpack(self.fmt, buffer[offset:new_offset])
        return unpacked_values[0], new_offset


class ProxyPacker(Packer):
    def __init__(
        self,
        cls: type,
        packer: Packer,
        to_proxy: Callable[[Any], Any] = bytes,
        to_cls: Callable[[Any], Any] = None,
    ) -> None:
        self.cls = cls
        self.packer = packer
        self.size = self.packer.size
        self.to_proxy = to_proxy
        if to_cls is None:
            to_cls = cls
        self.to_cls = to_cls

    def pack(self, v: Any) -> bytes:
        return self.packer.pack(self.to_proxy(v))

    def unpack(self, buffer: Buffer, offset: int) -> Tuple[Any, int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        v, new_offset = self.packer.unpack(buffer, offset)
        return self.to_cls(v), new_offset


class GreedyListPacker(Packer):
    def __init__(
        self,
        item_cls: type,
        item_packer: Packer = None,
        packer_lib: "PackerLibrary" = None,
    ) -> None:
        self.cls = list
        self.item_cls = item_cls
        if item_packer is None:
            self.item_packer = packer_lib.get_packer_by_type(item_cls)
        else:
            self.item_packer = item_packer
        self.size = None

    def pack(self, v: List[Any]) -> bytes:
        return b"".join(map(self.item_packer.pack, v))

    def unpack(self, buffer: Buffer, offset: int) -> Tuple[Any, int]:
        items = []
        while offset < len(buffer):
            v, offset = self.item_packer.unpack(buffer, offset)
            items.append(v)
        assert offset == len(buffer)
        return items, offset


class TuplePacker(Packer):
    def __init__(self, *packers: Packer, cls=tuple) -> None:
        self.packers = packers
        self.cls = cls
        if is_NamedTuple(cls):
            self.factory = lambda values: cls(*values)
        else:
            self.factory = lambda values: cls(values)
        try:
            self.size = sum(map(lambda p: p.size, packers))
        except TypeError:  # expected on `size==None`
            self.size = None

    def pack(self, values: tuple) -> bytes:
        tuple_size = len(self.packers)
        if tuple_size == len(values):
            return b"".join(self.packers[i].pack(values[i]) for i in range(tuple_size))
        else:
            raise AssertionError(f"size mismatch {tuple_size}: {values}")

    def unpack(self, buffer: Buffer, offset: int) -> Tuple[tuple, int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        values = []
        for p in self.packers:
            v, offset = p.unpack(buffer, offset)
            values.append(v)
        return self.factory(values), offset


INT_8 = TypePacker(int, "B")
INT_16 = TypePacker(int, "<H")
INT_32 = TypePacker(int, "<L")
INT_64 = TypePacker(int, "<Q")
BE_INT_64 = TypePacker(int, ">Q")
FLOAT = TypePacker(float, "<f")
DOUBLE = TypePacker(float, "<d")
ADJSIZE_PACKER_3 = AdjustableSizePacker(3)
ADJSIZE_PACKER_4 = AdjustableSizePacker(4)
SMALL_SIZED_BYTES = SizedPacker(ADJSIZE_PACKER_3)  # up to 2Mb
SIZED_BYTES = SizedPacker(ADJSIZE_PACKER_4)  # up to 256Mb
INT_32_SIZED_BYTES = SizedPacker(INT_32)
BOOL_AS_BYTE = ProxyPacker(bool, INT_8, int)

NANOTIME = ProxyPacker(nanotime, BE_INT_64, lambda nt: nt.nanoseconds(), nanotime)

UTC_DATETIME = ProxyPacker(
    datetime,
    DOUBLE,
    lambda dt: dt.replace(tzinfo=timezone.utc).timestamp(),
    datetime.utcfromtimestamp,
)

UTF8_STR = ProxyPacker(str, SIZED_BYTES, utf8_encode, utf8_decode)

GREEDY_BYTES = GreedyBytesPacker()
UTF8_GREEDY_STR = ProxyPacker(str, GREEDY_BYTES, utf8_encode, utf8_decode)


def build_code_enum_packer(code_enum_cls) -> Packer:
    return ProxyPacker(code_enum_cls, INT_8, int)


def unpack_constraining_greed(
    buffer: Buffer, offset: int, size: int, greedy_packer: Packer
) -> Tuple[Any, int]:
    """
    >>> unpack_constraining_greed(b'abc', 0, 3, UTF8_GREEDY_STR)
    ('abc', 3)
    >>> unpack_constraining_greed(b'abc', 1, 1, UTF8_GREEDY_STR)
    ('b', 2)
    >>> unpack_constraining_greed(b'abc', 0, 2, UTF8_GREEDY_STR)
    ('ab', 2)
    >>> unpack_constraining_greed(b'abc', 0, 10, UTF8_GREEDY_STR)
    Traceback (most recent call last):
    ...
    hashkernel.packer.NeedMoreBytes: 7
    >>> UTF8_GREEDY_STR.pack('abc')
    b'abc'
    """
    new_buffer, new_offset = FixedSizePacker(size).unpack(buffer, offset)
    return greedy_packer.unpack_whole_buffer(new_buffer), new_offset


PackerFactory = Callable[[type], Packer]


def named_tuple_packer(*parts: Packer):
    def factory(cls: type):
        return TuplePacker(*parts, cls=cls)

    return factory


class PackerLibrary:

    factories: List[Tuple[type, PackerFactory]]
    cache: Dict[type, Packer]

    def __init__(self, next_lib: "PackerLibrary" = None):
        self.factories = []
        self.cache = {}
        self.next_lib = next_lib

    def __contains__(self, item):
        return self[item] is not None

    def __getitem__(self, key: type) -> Packer:
        return self.get_packer_by_type(key)

    def get_packer_by_type(self, key: type) -> Packer:
        packer = None
        if key in self.cache:
            return self.cache[key]
        else:
            for i in range(len(self.factories)):
                factory_cls, factory = self.factories[i]
                if is_subclass(key, factory_cls):
                    packer = factory(key)
                    self.cache[key] = packer
                    return packer
            if packer is None and self.next_lib is not None:
                return self.next_lib.get_packer_by_type(key)
        raise KeyError(key)

    def resolve(self, key_cls: type):
        """
        decorator that make sure that PackerLibrary is capable to
        build packer of particular `key_cls`

        :param key_cls:
        :return:
        """
        self.get_packer_by_type(key_cls)
        return key_cls

    def register(self, packer: Union[PackerFactory, Packer]):
        """
        decorator that register particular `packer` with `key_cls`
        in library
        :param packer:
        :return:
        """

        def decorate(key_cls: type):
            self.register_packer(key_cls, packer)
            return key_cls

        return decorate

    def register_packer(self, key: type, packer: Union[PackerFactory, Packer]):
        self.cache = {}
        packer_factory = (lambda _: packer) if isinstance(packer, Packer) else packer
        for i in range(len(self.factories)):
            i_type = self.factories[i][0]
            assert i_type != key, "Conflict: Registering {key} twice"
            if is_subclass(key, i_type):
                self.factories.insert(i, (key, packer_factory))
                return
        self.factories.append((key, packer_factory))

    def register_all(self, *pack_defs: Tuple[type, Union[PackerFactory, Packer]]):
        for t in pack_defs:
            self.register_packer(*t)
        return self


def ensure_packer(o: Any, packerlib: PackerLibrary = None) -> Optional[Packer]:
    """
    >>> class A:
    ...     def __init__(self,i): self.i = int(i)
    ...     def __int__(self): return i
    ...     def __str__(self): return f'{i}'
    ...
    >>> A.__packer__ = ProxyPacker(A,INT_32,int)
    >>> ensure_packer(A) == A.__packer__
    True
    >>> ensure_packer(A.__packer__) == A.__packer__
    True
    >>> s_packer = ProxyPacker(A,UTF8_STR,str)
    >>> l = PackerLibrary()
    >>> l.register_packer(A,s_packer)
    >>> ensure_packer(A,l) == s_packer
    True
    """
    if isinstance(o, Packer) or o is None:
        return o
    elif isinstance(o, type) and packerlib is not None and o in packerlib:
        return packerlib[o]
    elif hasattr(o, "__packer__") and isinstance(o.__packer__, Packer):
        return o.__packer__
    return None


class PackerDefinitions:
    typed_packers: List[Tuple[type, PackerFactory]]

    def __init__(self, *pack_defs: Tuple[type, PackerFactory]):
        self.typed_packers = [*pack_defs]

    def build_lib(self, next_lib: PackerLibrary = None) -> PackerLibrary:
        return PackerLibrary(next_lib).register_all(*self)

    def __iter__(self):
        return iter(self.typed_packers)

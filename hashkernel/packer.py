import abc
import struct
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Tuple

from nanotime import nanotime

from hashkernel import OneBit, utf8_decode, utf8_encode
from hashkernel.typings import is_NamedTuple


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

    @abc.abstractmethod
    def pack(self, v: Any) -> bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def unpack(self, buffer: bytes, offset: int) -> Tuple[Any, int]:
        raise NotImplementedError("subclasses must override")


MARK_BIT = OneBit(7)


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

    def unpack(self, buffer: bytes, offset: int) -> Tuple[int, int]:
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

    def unpack(self, buffer: bytes, offset: int) -> Tuple[bytes, int]:
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

    def unpack(self, buffer: bytes, offset: int) -> Tuple[bytes, int]:
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

    def unpack(self, buffer: bytes, offset: int) -> Tuple[bytes, int]:
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

    def unpack(self, buffer: bytes, offset: int) -> Tuple[Any, int]:
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

    def unpack(self, buffer: bytes, offset: int) -> Tuple[Any, int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        v, new_offset = self.packer.unpack(buffer, offset)
        return self.to_cls(v), new_offset


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

    def unpack(self, buffer: bytes, offset: int) -> Tuple[tuple, int]:
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


def unpack_greedily(
    buffer: bytes, offset: int, size: int, greedy_packer: Packer
) -> Tuple[Any, int]:
    """
    >>> unpack_greedily(b'abc', 0, 3, UTF8_GREEDY_STR)
    ('abc', 3)
    >>> unpack_greedily(b'abc', 1, 1, UTF8_GREEDY_STR)
    ('b', 2)
    >>> unpack_greedily(b'abc', 0, 2, UTF8_GREEDY_STR)
    ('ab', 2)
    >>> unpack_greedily(b'abc', 0, 10, UTF8_GREEDY_STR)
    Traceback (most recent call last):
    ...
    hashkernel.packer.NeedMoreBytes: 7
    >>> UTF8_GREEDY_STR.pack('abc')
    b'abc'
    """
    new_buffer, new_offset = FixedSizePacker(size).unpack(buffer, offset)
    result, _ = greedy_packer.unpack(new_buffer, 0)
    return result, new_offset


def ensure_packer(o: Any) -> Packer:
    """
    >>> class A:
    ...     def __init__(self,i): self.i = i
    ...     def __int__(self): return i
    ...
    >>> A.__packer__ = ProxyPacker(A,INT_32,int)
    >>> ensure_packer(A) == A.__packer__
    True
    >>> ensure_packer(A.__packer__) == A.__packer__
    True
    """
    if isinstance(o, Packer):
        return o
    elif hasattr(o, "__packer__") and isinstance(o.__packer__, Packer):
        return o.__packer__
    raise AssertionError(f"Cannot extract packer out: {repr(o)}")

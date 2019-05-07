from typing import Any, Tuple, Callable

from datetime import datetime, timezone
from nanotime import nanotime
from hashkernel import (utf8_encode, utf8_decode)
import abc
import struct


class NeedMoreBytes(Exception):
    def __init__(self, how_much:int=None):
        self.how_much = how_much

    @staticmethod
    def check_underflow(buff_len, fragment_end)->int:
        if buff_len < fragment_end:
            raise NeedMoreBytes(fragment_end - buff_len)
        return fragment_end



SIGNIFICANT_BITS = 7
SEVEN_BITS = 0xFF >> (8 - SIGNIFICANT_BITS)
END_BIT = 0xFF ^ SEVEN_BITS


def size2bytes(sz:int)->bytes:
    """
    >>> size2bytes(3).hex()
    '83'
    >>> size2bytes(127).hex()
    'ff'
    >>> size2bytes(128).hex()
    '0081'
    >>> size2bytes(255).hex()
    '7f81'
    >>> size2bytes(16000).hex()
    '00fd'
    >>> size2bytes(17001).hex()
    '690481'
    >>> size2bytes(2000000).hex()
    '0009fa'
    >>> size2bytes(3000000).hex()
    Traceback (most recent call last):
    ...
    ValueError: Size is too big: 3000000
    """
    sz_bytes = []
    shift = sz
    for _ in range(3):
        numerical = shift & SEVEN_BITS
        shift = shift >> SIGNIFICANT_BITS
        if 0 == shift:
            sz_bytes.append(numerical | END_BIT)
            return bytes(sz_bytes)
        else:
            sz_bytes.append(numerical)
    raise ValueError(f'Size is too big: {sz}')


def pack_size(b: Any)->bytes:
    """
    >>> pack_size(bytes(b'abc'))
    b'\x83'
    >>> pack_size('abc')
    Traceback (most recent call last):
    ...
    ValueError: expected bytes not <class 'str'>
    """
    if isinstance(b, bytes):
        sz = len(b)
        return size2bytes(sz)
    else:
        raise ValueError(f'expected bytes not {type(b)}')


def unpack_size(buff:bytes, offset:int)->Tuple[int,int]:
    """
    >>> unpack_size(bytes([0x83]), 0)
    (3, 1)
    >>> unpack_size(bytes([0xff]), 0)
    (127, 1)
    >>> unpack_size(bytes([0x00,0x81]), 0)
    (128, 2)
    >>> unpack_size(bytes([0x7f,0x81]), 0)
    (255, 2)
    >>> unpack_size(bytes([0x00,0xfd]),0)
    (16000, 2)
    >>> unpack_size(bytes([0x69,0x04,0x81]),0)
    (17001, 3)
    >>> unpack_size(bytes([0x00,0x09,0xfa]),0)
    (2000000, 3)
    >>> unpack_size(bytes([0x00,0x09,0x7a,0x81]),0)
    Traceback (most recent call last):
    ...
    ValueError: No end bit

    >>> unpack_size(bytes([0x00,0x09]),0)
    Traceback (most recent call last):
    ...
    hashkernel.packer.NeedMoreBytes: 1

    Returns:
        size: Unpacked size
        new_offset: new offset in buffer

    """
    sz = 0
    buff_len = len(buff)
    for i in range(3):
        NeedMoreBytes.check_underflow(buff_len, offset + i + 1)
        v = buff[offset+i]
        end = v & END_BIT
        sz += (v & SEVEN_BITS) << (i * SIGNIFICANT_BITS)
        if end:
            return (sz, offset+i+1)
    raise ValueError("No end bit")


class Packer(metaclass=abc.ABCMeta):
    cls:type

    @abc.abstractmethod
    def pack(self, v:Any)->bytes:
        raise NotImplementedError('subclasses must override')

    @abc.abstractmethod
    def unpack(self, buffer:bytes, offset: int) -> Tuple[Any,int]:
        raise NotImplementedError('subclasses must override')

    @abc.abstractmethod
    def skip(self, buffer:bytes, offset: int) -> int:
        raise NotImplementedError('subclasses must override')


class SizedPacker(Packer):
    cls = bytes

    def pack(self, v:bytes)->bytes:
        return pack_size(v) + v

    def unpack(self, buffer:bytes, offset: int) -> Tuple[bytes,int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        size, data_offset = unpack_size(buffer, offset)
        new_offset = NeedMoreBytes.check_underflow(
            len(buffer), data_offset + size)
        return buffer[data_offset:new_offset], new_offset

    def skip(self, buffer: bytes, offset: int) -> int:
        """
        Returns:
              new offset in buffer
        """
        size, data_offset = unpack_size(buffer, offset)
        return NeedMoreBytes.check_underflow(
            len(buffer), data_offset + size)


class FixedSizePacker(Packer):
    cls = bytes

    def __init__(self, size: int)->None:
        self.size = size

    def pack(self, v:bytes)->bytes:
        if len(v) == self.size:
            return v
        else:
            raise AssertionError(f'{len(v)} != {self.size}')

    def unpack(self, buffer:bytes, offset: int) -> Tuple[bytes,int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        new_offset = offset+self.size
        NeedMoreBytes.check_underflow(len(buffer), new_offset)
        return buffer[offset:new_offset], new_offset

    def skip(self, buffer: bytes, offset: int) -> int:
        """
        Returns:
              new offset in buffer
        """
        return NeedMoreBytes.check_underflow(
            len(buffer), offset + self.size)


class TypePacker(Packer):

    def __init__(self, cls:type, fmt:str)->None:
        self.cls = cls
        self.fmt = fmt
        self.size = struct.calcsize(self.fmt)

    def pack(self, v:Any)->bytes:
        return struct.pack(self.fmt, v)

    def unpack(self, buffer:bytes, offset: int) -> Tuple[Any,int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        new_offset = self.size + offset
        NeedMoreBytes.check_underflow(len(buffer), new_offset)
        unpacked_values = struct.unpack(self.fmt, buffer[offset:new_offset])
        return unpacked_values[0], new_offset

    def skip(self, buffer: bytes, offset: int) -> int:
        """
        Returns:
              new offset in buffer
        """
        return NeedMoreBytes.check_underflow(
            len(buffer), offset + self.size)


class ProxyPacker(Packer):

    def __init__(self, cls: type, packer: Packer,
                 in_callback: Callable[[Any],Any],
                 out_callback: Callable[[Any],Any])->None:
        self.cls = cls
        self.packer = packer
        self.in_callback = in_callback
        self.out_callback = out_callback

    def pack(self, v: Any)->bytes:
        return self.packer.pack( self.in_callback(v) )

    def unpack(self, buffer: bytes, offset: int) -> Tuple[Any,int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        v, new_offset = self.packer.unpack(buffer, offset)
        return self.out_callback(v), new_offset

    def skip(self, buffer: bytes, offset: int) -> int:
        """
        Returns:
              new offset in buffer
        """
        return self.packer.skip(buffer, offset)


class TuplePacker(Packer):
    cls = tuple

    def __init__(self, *packers: Packer)->None:
        self.packers = packers

    def pack(self, values: tuple)->bytes:
        tuple_size = len(self.packers)
        if tuple_size == len(values):
            return b''.join(
                self.packers[i].pack(values[i])
                for i in range(tuple_size))
        else:
            raise AssertionError(
                f'size mismatch {tuple_size}: {values}')

    def unpack(self, buffer: bytes, offset: int) -> Tuple[tuple,int]:
        """
        Returns:
              value: unpacked value
              new_offset: new offset in buffer
        """
        values = []
        for p in self.packers:
            v, offset = p.unpack(buffer, offset)
            values.append(v)
        return tuple(values), offset

    def skip(self, buffer: bytes, offset: int) -> int:
        """
        Returns:
              new offset in buffer
        """
        for p in self.packers:
            offset = p.skip(buffer, offset)
        return offset


INT_8 = TypePacker(int, "B")
INT_16 = TypePacker(int, "<H")
INT_32 = TypePacker(int, "<L")
INT_64 = TypePacker(int, "<Q")
BE_INT_64 = TypePacker(int, ">Q")
FLOAT = TypePacker(float, "<f")
DOUBLE = TypePacker(float, "<d")
SIZED_BYTES = SizedPacker()

NANOTIME = ProxyPacker(nanotime, BE_INT_64,
                       lambda nt: nt.nanoseconds(),
                       nanotime )

UTC_DATETIME = ProxyPacker(datetime, DOUBLE,
                      lambda dt: dt.replace(tzinfo=timezone.utc).timestamp(),
                      datetime.utcfromtimestamp)

UTF8_STR = ProxyPacker(str, SIZED_BYTES, utf8_encode, utf8_decode)


def ensure_packer(o:Any) -> Packer:
    if isinstance(o, Packer):
        return o
    elif hasattr(o, '__packer__') and isinstance(o.__packer__, Packer):
        return o.__packer__
    raise AssertionError(f'Cannot extract packer out: {repr(o)}')
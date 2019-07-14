import os
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union

import pytz
from nanotime import datetime as datetime2nanotime
from nanotime import nanotime

from hashkernel.packer import INT_8, NANOTIME


def _nt_offset(i: int) -> int:
    return 1 << (33 + (i & 0x1F))


MAX_NANOSECONDS = 0xFFFFFFFFFFFFFFFF

_TIMEDELTAS = [timedelta(seconds=nanotime(_nt_offset(i)).seconds()) for i in range(31)]

RANDOM_PART_SIZE = 23


def new_guid_data(ttl: Union[nanotime, timedelta, None] = None):
    return pack_now(ttl) + os.urandom(RANDOM_PART_SIZE)


def control_byte(ttl: Union[nanotime, timedelta, None] = None) -> bytes:
    """
    3 upper bits reserved for future use, 5 lower bits store TTL
    in exponential form: interval equal 1 << (five_bit_int + 33) ns

    TTL never expires
    >>> control_byte().hex()
    '1f'

    >>> control_byte(timedelta(seconds=5)).hex()
    '00'
    >>> control_byte(timedelta(seconds=10)).hex()
    '01'
    >>> control_byte(timedelta(days=10)).hex()
    '11'
    >>> control_byte(timedelta(days=365*200)).hex()
    '1e'
    >>> control_byte(timedelta(days=365*300)).hex()
    '1f'
    >>> control_byte(nanotime(576460000*1e9)).hex()
    '1a'
    """
    idx = 31
    if isinstance(ttl, nanotime):
        ttl = timedelta(seconds=ttl.seconds())
    if isinstance(ttl, timedelta):
        idx = 0
        for td in _TIMEDELTAS:
            if ttl < td:
                break
            idx += 1
    else:
        assert ttl is None
    return bytes([idx])


def pack_now(ttl: Union[nanotime, timedelta, None] = None) -> bytes:
    """
    pack nanotime_now + control_byte

    >>> len(pack_now())
    9
    >>> type(pack_now())
    <class 'bytes'>

    :return: pack utc time into 9 bytes
    """
    return NANOTIME.pack(nanotime_now()) + control_byte(ttl)


def nanotime_now():
    return datetime2nanotime(datetime.utcnow())


def nanotime2datetime(nt: nanotime) -> datetime:
    """
    >>> nanotime2datetime(nanotime(0))
    datetime.datetime(1970, 1, 1, 0, 0)
    """
    return datetime.utcfromtimestamp(nt.timestamp())


def guid_to_nanotime(guid: bytes) -> nanotime:
    """
    >>> nt_before = nanotime_now()
    >>> data = new_guid_data()
    >>> len(data)
    32
    >>> nt = guid_to_nanotime(data)
    >>> nt_after = nanotime_now()
    >>> nt_before.nanoseconds() <= nt.nanoseconds()
    True
    >>> nt.nanoseconds() <= nt_after.nanoseconds()
    True
    >>>

    :param guid:
    :return: time extracted from guid
    """
    nt, _ = NANOTIME.unpack(guid, 0)
    return nt


def extract_timing(guid: bytes) -> Tuple[nanotime, Optional[nanotime]]:
    """
    >>> nt_before = nanotime_now()
    >>> data = new_guid_data()
    >>> len(data)
    32
    >>> nt,ttl = extract_timing(data)
    >>> nt_after = nanotime_now()
    >>> nt_before.nanoseconds() <= nt.nanoseconds()
    True
    >>> nt.nanoseconds() <= nt_after.nanoseconds()
    True
    >>> ttl is None
    True

    >>> nt_before = nanotime_now()
    >>> data = new_guid_data(timedelta(days=10))
    >>> len(data)
    32
    >>> nt,ttl = extract_timing(data)
    >>> nt_after = nanotime_now()
    >>> nt_before.nanoseconds() <= nt.nanoseconds()
    True
    >>> nt.nanoseconds() <= nt_after.nanoseconds()
    True
    >>> ttl_delta = (nanotime2datetime(ttl)-nanotime2datetime(nt))
    >>> timedelta(days=10) <= ttl_delta
    True
    >>> ttl_delta.days
    13


    Returns:
        created_nt - creation time
        ttl_nt - TTL
    """
    nt, offset = NANOTIME.unpack(guid, 0)
    ctl, _ = INT_8.unpack(guid, offset)
    ttl_ns = nt.nanoseconds() + _nt_offset(ctl)
    return nt, None if ttl_ns >= MAX_NANOSECONDS else nanotime(ttl_ns)

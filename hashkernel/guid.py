import os
from datetime import timedelta
from typing import Union

from nanotime import nanotime

from hashkernel.time import nano_ttl, nanotime_now

RANDOM_PART_SIZE = 23


def new_guid_data(ttl: Union[nanotime, timedelta, None] = None):
    return pack_now(ttl) + os.urandom(RANDOM_PART_SIZE)


def pack_now(ttl: Union[nanotime, timedelta, None] = None) -> bytes:
    """
    pack nanotime_now + control_byte

    >>> len(pack_now())
    9
    >>> type(pack_now())
    <class 'bytes'>

    :return: pack utc time into 9 bytes
    """
    return bytes(nano_ttl(nanotime_now(), ttl))


def guid_to_nano_ttl(guid: bytes) -> nano_ttl:
    """
    >>> nt_before = nanotime_now()
    >>> data = new_guid_data()
    >>> len(data)
    32
    >>> nt_ttl = guid_to_nano_ttl(data)
    >>> nt_after = nanotime_now()
    >>> nt_before.nanoseconds() <= nt_ttl.time.nanoseconds()
    True
    >>> nt_ttl.time.nanoseconds() <= nt_after.nanoseconds()
    True

    :param guid:
    :return: time extracted from guid
    """
    return nano_ttl(guid)

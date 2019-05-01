from datetime import datetime
import os
from nanotime import nanotime, datetime as datetime2nanotime
from hashkernel.packer import NANOTIME


def new_guid_data():
    return pack_now() + os.urandom(24)


def pack_now()->bytes:
    """

    >>> len(pack_now())
    8
    >>> type(pack_now())
    <class 'bytes'>

    :return: pack utc time into 8 bytes
    """
    return NANOTIME.pack(nanotime_now())


def nanotime_now():
    return datetime2nanotime(datetime.utcnow())


def guid_to_nanotime(guid: bytes)->nanotime:
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

    :param guid:
    :return: time extracted from guid
    """
    nt, _ = NANOTIME.unpack(guid, 0)
    return nt

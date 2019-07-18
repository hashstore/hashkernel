from datetime import datetime, timedelta
from typing import Union

import pytz
from croniter import croniter
from nanotime import datetime as datetime2nanotime
from nanotime import nanotime

from hashkernel import EnsureIt, Stringable, StrKeyMixin
from hashkernel.packer import INT_8, NANOTIME, TuplePacker


def nanotime2datetime(nt: nanotime) -> datetime:
    """
    >>> nanotime2datetime(nanotime(0))
    datetime.datetime(1970, 1, 1, 0, 0)
    """
    return datetime.utcfromtimestamp(nt.timestamp())


def _nt_offset(i: int) -> int:
    return 1 << (33 + (i & 0x1F))


def nanotime_now():
    return datetime2nanotime(datetime.utcnow())


FOREVER = nanotime(0xFFFFFFFFFFFFFFFF)

_TIMEDELTAS = [timedelta(seconds=nanotime(_nt_offset(i)).seconds()) for i in range(31)]


def ttl_idx(ttl: Union[nanotime, timedelta, None] = None) -> int:
    """
    TTL never expires
    >>> ttl_idx()
    31
    >>> ttl_idx(timedelta(seconds=5))
    0
    >>> ttl_idx(timedelta(seconds=10))
    1
    >>> ttl_idx(timedelta(days=10))
    17
    >>> ttl_idx(timedelta(days=365*200))
    30

    Too far in future means never expires
    >>> ttl_idx(timedelta(days=365*300))
    31

    >>> ttl_idx(nanotime(576460000*1e9))
    26

    MAYBE: switch to binary search and see how it improve
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
    return idx


TTL_TUPLE_PACKER = TuplePacker(NANOTIME, INT_8)


class nano_ttl:
    """
    >>> nano_ttl.SIZEOF
    9
    >>> nt = nano_ttl(nanotime_now())
    >>> nt.ttl() == FOREVER
    True

    >>> nt_before = nanotime_now()
    >>> nt = nano_ttl(nanotime_now(), timedelta(days=10))
    >>> ttl_delta = (nanotime2datetime(nt.ttl())-nanotime2datetime(nt.time))
    >>> timedelta(days=10) <= ttl_delta
    True
    >>> ttl_delta.days
    13

    """

    SIZEOF = TTL_TUPLE_PACKER.size

    time: nanotime
    ttl_idx: int

    def __init__(
        self,
        t: Union[datetime, nanotime, bytes],
        ttl: Union[int, nanotime, timedelta, None] = None,
    ):
        if isinstance(t, bytes):
            assert ttl is None
            (self.time, self.ttl_idx), _ = TTL_TUPLE_PACKER.unpack(t, 0)
        else:
            self.time = datetime2nanotime(t) if isinstance(t, datetime) else t
            self.ttl_idx = ttl if isinstance(ttl, int) else ttl_idx(ttl)

    def ttl(self) -> nanotime:
        ttl_ns = self.time.nanoseconds() + _nt_offset(self.ttl_idx)
        return FOREVER if ttl_ns >= FOREVER.nanoseconds() else nanotime(ttl_ns)

    def __bytes__(self):
        return TTL_TUPLE_PACKER.pack((self.time, self.ttl_idx))


class CronExp(Stringable, EnsureIt, StrKeyMixin):
    """
    >>> c = CronExp('* * 9 * *')
    >>> c
    CronExp('* * 9 * *')
    >>> str(c)
    '* * 9 * *'
    """

    def __init__(self, s):
        self.exp = s
        self.croniter()

    def croniter(self, dt=None):
        return croniter(self.exp, dt)

    def __str__(self):
        return self.exp


class TimeZone(Stringable, EnsureIt, StrKeyMixin):
    """
    >>> c = TimeZone('Asia/Tokyo')
    >>> c
    TimeZone('Asia/Tokyo')
    >>> str(c)
    'Asia/Tokyo'
    >>> TimeZone('Asia/Toky') # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    UnknownTimeZoneError: 'Asia/Toky'
    """

    def __init__(self, s):
        self.tzName = s
        self.tz()

    def tz(self):
        return pytz.timezone(self.tzName)

    def __str__(self):
        return self.tzName

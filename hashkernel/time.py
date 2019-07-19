from datetime import datetime, timedelta
from typing import Union

import pytz
from croniter import croniter
from nanotime import datetime as datetime2nanotime
from nanotime import nanotime

from hashkernel import EnsureIt, Stringable, StrKeyMixin
from hashkernel.packer import INT_8, NANOTIME, ProxyPacker, TuplePacker


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
_MASKS = [1 << i for i in range(4, -1, -1)]


class TTL:
    """
    TTL never expires
    >>> TTL().idx
    31
    >>> TTL(timedelta(seconds=5)).idx
    0
    >>> TTL(timedelta(seconds=10)).idx
    1
    >>> TTL(timedelta(days=10)).idx
    17
    >>> TTL(timedelta(days=365*200)).idx
    30

    Too far in future means never expires
    >>> TTL(timedelta(days=365*300)).idx
    31

    >>> TTL(nanotime(576460000*1e9)).idx
    26

    """

    idx: int

    def __init__(self, ttl: Union[int, nanotime, timedelta, None] = None) -> int:
        idx = 31
        if isinstance(ttl, nanotime):
            ttl = timedelta(seconds=ttl.seconds())
        if isinstance(ttl, timedelta):
            idx = 0
            for m in _MASKS:
                c = _TIMEDELTAS[idx + m - 1]
                if ttl > c:
                    idx += m
        elif isinstance(ttl, int):
            idx = ttl
        else:
            assert ttl is None, f"{ttl}"
        assert idx < 32 and idx >= 0
        self.idx = idx

    def timedelta(self):
        return _TIMEDELTAS[self.idx]

    def expires(self, t: nanotime) -> nanotime:
        ns = t.nanoseconds() + _nt_offset(self.idx)
        return FOREVER if ns >= FOREVER.nanoseconds() else nanotime(ns)

    def __int__(self):
        return self.idx


TTL_PACKER = ProxyPacker(TTL, INT_8, int)

NANOTTL_TUPLE_PACKER = TuplePacker(NANOTIME, TTL_PACKER)


class nano_ttl:
    """
    >>> nano_ttl.SIZEOF
    9
    >>> nt = nano_ttl(nanotime_now())
    >>> nt.time_expires() == FOREVER
    True

    >>> nt_before = nanotime_now()
    >>> nt = nano_ttl(nanotime_now(), timedelta(days=10))
    >>> ttl_delta = (nanotime2datetime(nt.time_expires())-nanotime2datetime(nt.time))
    >>> timedelta(days=10) <= ttl_delta
    True
    >>> ttl_delta.days
    13

    """

    SIZEOF = NANOTTL_TUPLE_PACKER.size

    time: nanotime
    ttl: TTL

    def __init__(
        self,
        t: Union[datetime, nanotime, bytes],
        ttl: Union[TTL, int, nanotime, timedelta, None] = None,
    ):
        if isinstance(t, bytes):
            assert ttl is None
            (self.time, self.ttl), _ = NANOTTL_TUPLE_PACKER.unpack(t, 0)
        else:
            self.time = datetime2nanotime(t) if isinstance(t, datetime) else t
            self.ttl = ttl if isinstance(ttl, TTL) else TTL(ttl)

    def time_expires(self) -> nanotime:
        return self.ttl.expires(self.time)

    def __bytes__(self):
        return NANOTTL_TUPLE_PACKER.pack((self.time, self.ttl))


NANO_TTL_PACKER = ProxyPacker(nano_ttl, NANOTTL_TUPLE_PACKER)


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

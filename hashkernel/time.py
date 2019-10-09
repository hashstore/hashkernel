from datetime import datetime, timedelta
from functools import total_ordering
from typing import Tuple, Union

import pytz
from croniter import croniter
from nanotime import datetime as datetime2nanotime
from nanotime import nanotime

from hashkernel import EnsureIt, Integerable, OneBit, Stringable, StrKeyMixin
from hashkernel.packer import INT_8, NANOTIME, ProxyPacker, TuplePacker


def nanotime2datetime(nt: nanotime) -> datetime:
    """
    >>> nanotime2datetime(nanotime(0))
    datetime.datetime(1970, 1, 1, 0, 0)
    """
    return datetime.utcfromtimestamp(nt.timestamp())


def split_ttl(i) -> Tuple[int, int]:
    """
    Splits TTL, that packs into 1 byte

    Returns:
        idx - lowest 5 bits
        extra - higest 3 bits

    >>> split_ttl(0xE5)
    (5, 7)
    >>> split_ttl(0x105)
    (5, 0)
    """
    return (i & 0x1F), (i & 0xE0) >> 5


def pack_ttl(ttl_idx, extra):
    """
    Packs TTL and extra into 1 byte. Inverse of `split_ttl()`

    >>> pack_ttl(5, 7) == 0xE5
    True
    >>> pack_ttl(5, 0)
    5
    """
    return (ttl_idx & 0x1F) + ((extra & 7) << 5)


def offset_in_ns(ttl_idx: int) -> int:
    return 1 << (33 + (ttl_idx & 0x1F))


def nanotime_now():
    return datetime2nanotime(datetime.utcnow())


FOREVER = nanotime(0xFFFFFFFFFFFFFFFF)
_TIMEDELTAS = [
    timedelta(seconds=nanotime(offset_in_ns(i)).seconds()) for i in range(31)
]
_MASKS = [1 << i for i in range(4, -1, -1)]


@total_ordering
class TTL(Integerable):
    """
    TTL - Time to live interval expressed in 0 - 31 integer

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

    Fifth TTL is about 5 minutes or in seconds
    >>> TTL(5).timedelta().seconds
    274
    >>> t5=TTL(5)
    >>> t5.get_extra_bit(OneBit(0))
    False
    >>> t5.set_extra_bit(OneBit(0), True)
    >>> t5.get_extra_bit(OneBit(0))
    True
    >>> t5.extra
    1
    >>> int(t5)
    37
    >>> copy=TTL(int(t5))
    >>> t5.set_extra_bit(OneBit(0), False)
    >>> t5.get_extra_bit(OneBit(0))
    False
    >>> copy.get_extra_bit(OneBit(0))
    True
    >>> t5 < copy
    True
    >>> t5 > copy
    False
    >>> t5 != copy
    True
    >>> t5 == copy
    False
    >>> t5.set_extra_bit(OneBit(0), True)
    >>> t5 == copy
    True
    >>> from hashkernel import json_encode, to_json
    >>> to_json(TTL(3))
    3
    >>> json_encode({"ttl": TTL(5)})
    '{"ttl": 5}'

    """

    idx: int
    extra: int

    def __init__(self, ttl: Union[int, nanotime, timedelta, None] = None):
        idx = 31
        extra = 0
        if isinstance(ttl, nanotime):
            ttl = timedelta(seconds=ttl.seconds())
        if isinstance(ttl, timedelta):
            idx = 0
            for m in _MASKS:
                c = _TIMEDELTAS[idx + m - 1]
                if ttl > c:
                    idx += m
        elif isinstance(ttl, int):
            idx, extra = split_ttl(ttl)
        else:
            assert ttl is None, f"{ttl}"
        assert 0 <= idx < 32
        self.idx = idx
        self.extra = extra

    def timedelta(self):
        return _TIMEDELTAS[self.idx]

    def expires(self, t: nanotime) -> nanotime:
        ns = t.nanoseconds() + offset_in_ns(self.idx)
        return FOREVER if ns >= FOREVER.nanoseconds() else nanotime(ns)

    def __eq__(self, other):
        return (self.idx, self.extra) == (other.idx, other.extra)

    def __lt__(self, other):
        return (self.idx, self.extra) < (other.idx, other.extra)

    def __int__(self):
        return pack_ttl(self.idx, self.extra)

    def get_extra_bit(self, bit: OneBit) -> bool:
        return bool(self.extra & bit.mask)

    def set_extra_bit(self, bit: OneBit, v: bool):
        self.extra = self.extra | bit.mask if v else self.extra & bit.inverse

    def __repr__(self):
        return f"TTL({int(self)})"

    @classmethod
    def all(cls):
        """
        All posible `TTL`s with extra bits set to 0
        """
        return (cls(i) for i in range(32))


FEW_SECONDS_TTL = TTL(0)  # 8.6s
ABOUT_A_MINUTE_TTL = TTL(timedelta(minutes=1))
ABOUT_AN_HOUR_TTL = TTL(timedelta(hours=1))
ABOUT_A_DAY_TTL = TTL(timedelta(days=1))
ABOUT_A_WEEK_TTL = TTL(timedelta(days=7))
ABOUT_A_MONTH_TTL = TTL(timedelta(days=30))
ABOUT_A_QUARTER_TTL = TTL(timedelta(days=90))
ABOUT_A_YEAR_TTL = TTL(timedelta(days=366))
FOREVER_TTL = TTL()

TTL_PACKER = ProxyPacker(TTL, INT_8, int)

NANOTTL_TUPLE_PACKER = TuplePacker(NANOTIME, TTL_PACKER)


@total_ordering
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
    >>> bytes(nano_ttl(nanotime(0x0102030405060708))).hex()
    '01020304050607081f'
    >>> nano_ttl(bytes(nt)) == nt
    True
    >>> from time import sleep; sleep(1.5e-3)
    >>> later = nano_ttl(nanotime_now(), timedelta(days=10))
    >>> later > nt
    True

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

    def __eq__(self, other):
        return (self.time.nanoseconds(), self.ttl) == (
            other.time.nanoseconds(),
            other.ttl,
        )

    def __lt__(self, other):
        return (self.time.nanoseconds(), self.ttl) < (
            other.time.nanoseconds(),
            other.ttl,
        )


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

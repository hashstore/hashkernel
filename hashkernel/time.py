from datetime import datetime, timedelta
from functools import total_ordering
from typing import Callable, Dict, Union, cast

import pytz
from croniter import croniter
from nanotime import datetime as datetime2nanotime
from nanotime import nanotime

from hashkernel import (
    BitMask,
    EnsureIt,
    Integerable,
    ScaleHelper,
    Scaling,
    Stringable,
    StrKeyMixin,
)
from hashkernel.packer import INT_8, NANOTIME, ProxyPacker, TuplePacker


def nanotime2datetime(nt: nanotime) -> datetime:
    """
    >>> nanotime2datetime(nanotime(0))
    datetime.datetime(1970, 1, 1, 0, 0)
    """
    return datetime.utcfromtimestamp(nt.timestamp())


def nanotime_now():
    return datetime2nanotime(datetime.utcnow())


FOREVER = nanotime(0xFFFFFFFFFFFFFFFF)
FOREVER_DELTA = timedelta(seconds=FOREVER.seconds())

_DELTA_EXTRACTORS: Dict[str, Callable[[timedelta], int]] = {
    "y": lambda td: int(td.days / 365),
    "d": lambda td: int(td.days % 365),
    "h": lambda td: int(td.seconds / 3600) % 24,
    "s": lambda td: int(td.seconds % 3600),
}


def delta2str(td: timedelta) -> str:
    s = ""
    for n, fn in _DELTA_EXTRACTORS.items():
        i = fn(td)
        if i > 0:
            s += f"{i}{n}"
    return s


class Timeout(Scaling):
    """
    Timeout - Time to live interval expressed in 0 - 15 integer

    Timeout that never expires (max nanotime from now)
    >>> str(Timeout(15))
    '584y343d23h2073s'
    >>> Timeout(15)
    Timeout(15)

    >>> t=Timeout.resolve(timedelta(seconds=5))
    >>> t.idx
    1
    >>> t.timedelta().seconds, t.timedelta().microseconds
    (5, 0)
    >>> Timeout.resolve(timedelta(seconds=10)).idx
    2
    >>> Timeout.resolve(timedelta(days=10)).idx
    9
    >>> td=Timeout.resolve(timedelta(days=10)).timedelta()
    >>> td.days, td.seconds
    (22, 52325)
    >>> Timeout.resolve(timedelta(days=365*100)).idx
    14

    Duration of intervals
    >>> list(map(str, Timeout.all())) #doctest: +NORMALIZE_WHITESPACE
    ['1s', '5s', '25s', '125s',
    '625s', '3125s', '4h1225s', '21h2525s',
    '4d12h1825s', '22d14h1925s', '113d2425s', '1y200d3h1325s',
    '7y270d16h3025s', '38y258d12h725s', '193y197d13h25s', '584y343d23h2073s']

    300 years is too far in future means never expires
    >>> Timeout.resolve(timedelta(days=365*300)).idx
    15

    >>> Timeout.resolve(nanotime(576460000*1e9)).idx
    13

    Fifth Timeout is about 5 minutes or in seconds
    >>> t5=Timeout.resolve(5)
    >>> t5.timedelta().seconds
    3125
    >>> copy=Timeout.resolve(int(t5)+1)
    >>> t5 < copy
    True
    >>> t5 > copy
    False
    >>> t5 != copy
    True
    >>> t5 == copy
    False
    >>> from hashkernel import json_encode, to_json
    >>> to_json(Timeout(3))
    3

    """

    @staticmethod
    def __new_scale_helper__():
        """
        Helper factory
        """
        return ScaleHelper(
            lambda i: FOREVER_DELTA if i == 15 else timedelta(seconds=5 ** i),
            bit_size=4,
        )

    @classmethod
    def resolve(cls, ttl: Union[int, nanotime, timedelta]) -> "Timeout":
        if isinstance(ttl, nanotime):
            ttl = timedelta(seconds=ttl.seconds())
        if isinstance(ttl, timedelta):
            return cast(Timeout, cls.search(ttl))
        else:
            return Timeout(ttl)

    def timedelta(self) -> timedelta:
        return self.value()

    def expires(self, t: nanotime) -> nanotime:
        ns = t.nanoseconds() + (5 ** self.idx) * 1e9
        return FOREVER if ns >= FOREVER.nanoseconds() else nanotime(ns)

    def __str__(self) -> str:
        return delta2str(self.timedelta())

    # def __repr__(self):
    #     return f"Timeout({int(self)})"


TTL_IDX = BitMask(0, 4)
TTL_EXTRA = BitMask(4, 4)


@total_ordering
class TTL(Integerable):
    """
    TTL - Time to live interval expressed in 0 - 15 integer

    TTL that never expires (max nanotime from now)
    >>> str(TTL())
    '584y343d23h2073s extra=0'
    >>> TTL()
    TTL(15)

    >>> t=TTL(timedelta(seconds=5))
    >>> t.timeout.idx
    1
    >>> t.timeout.timedelta().seconds, t.timeout.timedelta().microseconds
    (5, 0)
    >>> TTL(timedelta(seconds=10)).timeout.idx
    2
    >>> TTL(timedelta(days=10)).timeout.idx
    9
    >>> td=TTL(timedelta(days=10)).timeout.timedelta()
    >>> td.days, td.seconds
    (22, 52325)
    >>> TTL(timedelta(days=365*100)).timeout.idx
    14

    300 years is too far in future means never expires
    >>> TTL(timedelta(days=365*300)).timeout.idx
    15

    >>> TTL(nanotime(576460000*1e9)).timeout.idx
    13

    Fifth TTL is about 5 minutes or in seconds
    >>> TTL(5).timeout.timedelta().seconds
    3125
    >>> t5=TTL(5)
    >>> t5.get_extra_bit(BitMask(0))
    False
    >>> t5.set_extra_bit(BitMask(0), True)
    >>> t5.get_extra_bit(BitMask(0))
    True
    >>> t5.extra
    1
    >>> int(t5)
    21
    >>> copy=TTL(int(t5))
    >>> t5.set_extra_bit(BitMask(0), False)
    >>> t5.get_extra_bit(BitMask(0))
    False
    >>> copy.get_extra_bit(BitMask(0))
    True
    >>> t5 < copy
    True
    >>> t5 > copy
    False
    >>> t5 != copy
    True
    >>> t5 == copy
    False
    >>> t5.set_extra_bit(BitMask(0), True)
    >>> t5 == copy
    True
    >>> from hashkernel import json_encode, to_json
    >>> to_json(TTL(3))
    3
    >>> json_encode({"ttl": TTL(5)})
    '{"ttl": 5}'

    """

    timeout: Timeout
    extra: int

    def __init__(self, ttl: Union[int, nanotime, timedelta, None] = None):
        if ttl is None:
            ttl = 15
        if isinstance(ttl, int):
            self.timeout = Timeout(TTL_IDX.extract(ttl))
            self.extra = TTL_EXTRA.extract(ttl)
        else:
            self.timeout = Timeout.resolve(ttl)
            self.extra = 0

    def __eq__(self, other):
        return (self.timeout, self.extra) == (other.timeout, other.extra)

    def __lt__(self, other):
        return (self.timeout, self.extra) < (other.timeout, other.extra)

    def __int__(self):
        return TTL_EXTRA.update(TTL_IDX.update(0, int(self.timeout)), self.extra)

    def get_extra_bit(self, bit: BitMask) -> bool:
        return bool(self.extra & bit.mask)

    def set_extra_bit(self, bit: BitMask, v: bool):
        self.extra = self.extra | bit.mask if v else self.extra & bit.inverse

    def __str__(self):
        return f"{self.timeout} extra={self.extra}"

    def __repr__(self):
        return f"TTL({int(self)})"

    @classmethod
    def all(cls):
        """
        All posible `TTL`s with extra bits set to 0
        """
        return (cls(i) for i in range(16))


HOURISH_TTL = TTL(5)  # 3125s
DAYISH_TTL = TTL(7)  # 21h2525s
WEEKISH_TTL = TTL(8)  # 4d12h1825s
MONTHISH_TTL = TTL(9)  # 22d14h1925s
QUARTERISH_TTL = TTL(10)  # 113d2425s
TWOYEARISH_TTL = TTL(11)  # 1y200d3h1325s
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
    22
    >>> bytes(nano_ttl(nanotime(0x0102030405060708))).hex()
    '01020304050607080f'
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
        return self.ttl.timeout.expires(self.time)

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
    >>> TimeZone('Asia/Toky')
    Traceback (most recent call last):
    ...
    pytz.exceptions.UnknownTimeZoneError: 'Asia/Toky'
    """

    def __init__(self, s):
        self.tzName = s
        self.tz()

    def tz(self):
        return pytz.timezone(self.tzName)

    def __str__(self):
        return self.tzName

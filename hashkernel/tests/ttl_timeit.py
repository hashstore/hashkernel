import datetime
import timeit

from hashkernel.base_x import base_x
from hashkernel.time import _TIMEDELTAS, TTL

B62 = base_x(62)


SECOND = datetime.timedelta(seconds=1)

ttls = [TTL] #, TTL2


def ttl(s):
    for i in range(31):
        td = _TIMEDELTAS[i]
        assert ttls[s](td).idx == i
        assert ttls[s](td - SECOND).idx == i
        assert ttls[s](td + SECOND).idx == i + 1


def do_timing(s):
    print(ttls[s].__name__)
    print(timeit.repeat(f"ttl({s})", "from __main__ import ttl", number=10000))


if __name__ == "__main__":
    for _ in range(4):
        for n in range(len(ttls)):
            do_timing(n)

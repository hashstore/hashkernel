import datetime
from logging import getLogger

from hashkernel.time import _TIMEDELTAS, TTL

SECOND = datetime.timedelta(seconds=1)

log = getLogger(__name__)


def test_TTL():
    for i in range(31):
        td = _TIMEDELTAS[i]
        before = td - SECOND
        after = td + SECOND
        assert TTL(before).idx == i
        assert TTL(after).idx == i + 1

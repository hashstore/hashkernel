import datetime
from logging import getLogger

import pytest

from hashkernel import ClassRef, Conversion, GlobalRef
from hashkernel.time import _TIMEDELTAS, TTL

SECOND = datetime.timedelta(seconds=1)

log = getLogger(__name__)


def all_timedeltas():
    return ((i, _TIMEDELTAS[i]) for i in range(31))


ttl_cref = ClassRef(TTL)


@pytest.mark.parametrize("ttl", TTL.all())
def test_with_cref(ttl):
    i = ttl_cref.convert(ttl, Conversion.TO_JSON)
    assert i == ttl.idx
    back_to_ttl = ttl_cref.convert(i, Conversion.TO_OBJECT)
    assert back_to_ttl == ttl
    assert ttl_cref.matches(ttl)
    assert ttl_cref.matches(back_to_ttl)
    assert not (ttl_cref.matches(i))
    assert ttl_cref.matches(TTL())


@pytest.mark.parametrize("i,td", all_timedeltas())
def test_TTL(i, td):
    before = td - SECOND
    after = td + SECOND
    assert TTL(td).idx == i
    assert TTL(before).idx == i
    assert TTL(after).idx == i + 1

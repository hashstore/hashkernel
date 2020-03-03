import datetime
from logging import getLogger

import pytest

from hashkernel.mold import ClassRef, Conversion
from hashkernel.time import TTL, Timeout

SECOND = datetime.timedelta(seconds=1)

log = getLogger(__name__)

ttl_cref = ClassRef(TTL)


@pytest.mark.parametrize("ttl", TTL.all())
def test_with_cref(ttl):
    i = ttl_cref.convert(ttl, Conversion.TO_JSON)
    assert i == ttl.timeout.idx
    back_to_ttl = ttl_cref.convert(i, Conversion.TO_OBJECT)
    assert back_to_ttl == ttl
    assert ttl_cref.matches(ttl)
    assert ttl_cref.matches(back_to_ttl)
    assert not (ttl_cref.matches(i))
    assert ttl_cref.matches(TTL())


@pytest.mark.parametrize("i,to", enumerate(Timeout.all()))
def test_Timeout(i, to: Timeout):
    td = to.timedelta()
    before = td - SECOND
    after = td + SECOND
    assert Timeout.resolve(td).idx == i
    assert Timeout.resolve(before).idx == i
    assert Timeout.resolve(after).idx == min(i + 1, Timeout.size() - 1)

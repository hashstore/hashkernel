from datetime import datetime
from logging import getLogger
from hs_build_tools.nose import eq_, ok_

log = getLogger(__name__)

import hashkernel.packer as p

def test_docs():
    import doctest
    r = doctest.testmod(p)
    ok_(r.attempted > 0, 'There is not doctests in module')
    eq_(r.failed,0)


def test_1():
    begining_of_time = datetime.utcfromtimestamp(0.)
    z = p.TuplePacker(
                p.UTC_DATETIME, p.DOUBLE, p.UTF8_STR, p.FLOAT,
                p.INT_32, p.INT_16, p.INT_8)
    pack = z.pack((begining_of_time, 13497439e-30, "Hello World!!!",
                   42.0, 1000, 1000, 244))
    eq_(pack.hex(),'000000000000000029f7654a4151303b8e48656c6c6f20576f726c6421212100002842e8030000e803f4')
    unpack,sz = z.unpack(pack, 0)
    eq_(unpack,(begining_of_time, 1.3497439e-23, 'Hello World!!!', 42.0, 1000, 1000, 244))
    eq_(len(pack),sz)

    z = p.TuplePacker(
            p.FixedSizePacker(3),
            p.UTC_DATETIME,
            p.INT_8)
    pack = z.pack((bytes([65,66,67]),begining_of_time, 17))
    eq_(pack.hex(),'414243000000000000000011')
    unpack,sz = z.unpack(pack, 0)
    eq_(unpack,(b'ABC',begining_of_time, 17))
    eq_(len(pack),sz)

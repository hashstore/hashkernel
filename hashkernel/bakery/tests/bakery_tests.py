#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import getLogger

from hashkernel.bakery import Cake, CakeHeaders

log = getLogger(__name__)


def test_CAKe():
    def do_test(c, s):
        u1 = Cake.from_bytes(c)
        assert s == str(u1)
        u1n = Cake(str(u1))
        u1b = Cake(bytes(u1))
        assert u1.digest == u1n.digest
        assert u1 == u1n
        assert u1.digest == u1b.digest
        assert u1 == u1b

    do_test(b"", "RZwTDmWjELXeEmMEb0eIIegKayGGUPNsuJweEPhlXi50")
    do_test(b"a" * 1, "M2wFVNEmDX1opXFXKYMsbTv6wtlFLKv406fP1Io9PHd0")
    do_test(b"a" * 2, "zAThEaG91LzDkgXFISdnmebew615YIbWchH4Ds0mMnA0")
    do_test(b"a" * 3, "A5JIzO4SkX4P9iGsDnDIMHq0LuGUToKaOoQXCFIFCJq0")
    do_test(b"a" * 32, "e8Q7QjIGj9zzpWVkah5IE8nbGQwLpPycM22s743K6BV0")
    do_test(b"a" * 33, "vzEjZ0FMxLvbBgui5dOjjUqOXkozRdpndWdkd8GFEvM0")

    do_test(b"a" * 46, "ofsWs1MD2bqX34KrhZDGpw8I2LGyrDadV90nYzThzPt0")

    d = Cake.new_guid(CakeHeaders.MOUNT_FOLDER)
    x = Cake.new_guid(CakeHeaders.MOUNT_FOLDER)
    z = Cake(str(d))
    assert z == d
    assert (z != d) == False
    assert z != x
    assert d != x
    assert z.header == d.header
    assert str(z) == str(d)

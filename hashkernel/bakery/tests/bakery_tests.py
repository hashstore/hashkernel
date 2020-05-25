#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import getLogger

import pytest
from pytest import raises

from hashkernel.ake import Cake, Rake, RootSchema

log = getLogger(__name__)


@pytest.mark.parametrize(
    "content, cake_s",
    [
        (b"", "RZwTDmWjELXeEmMEb0eIIegKayGGUPNsuJweEPhlXi5"),
        (b"a" * 1, "M2wFVNEmDX1opXFXKYMsbTv6wtlFLKv406fP1Io9PHd"),
        (b"a" * 2, "zAThEaG91LzDkgXFISdnmebew615YIbWchH4Ds0mMnA"),
        (b"a" * 3, "A5JIzO4SkX4P9iGsDnDIMHq0LuGUToKaOoQXCFIFCJq"),
        (b"a" * 32, "e8Q7QjIGj9zzpWVkah5IE8nbGQwLpPycM22s743K6BV"),
        (b"a" * 33, "vzEjZ0FMxLvbBgui5dOjjUqOXkozRdpndWdkd8GFEvM"),
        (b"a" * 46, "ofsWs1MD2bqX34KrhZDGpw8I2LGyrDadV90nYzThzPt"),
    ],
)
def test_cake_roundtrip(content, cake_s):
    u1 = Cake.from_bytes(content)
    assert cake_s == str(u1)
    u1n = Cake(str(u1))
    u1b = Cake(bytes(u1))
    assert u1 == u1n
    assert u1 == u1b


def test_guid():
    d = Rake.build_new(RootSchema.CASKADE)
    x = Rake.build_new(RootSchema.CASKADE)

    z = Rake(str(d))
    assert z == d
    assert (z != d) == False
    assert z != x
    assert z.obj_type() == d.obj_type()

    z = Rake(bytes(d))
    assert z == d
    assert (z != d) == False
    assert z != x
    assert z.obj_type() == d.obj_type()

    assert d != x
    assert str(z) == str(d)


def test_wrong_size_of_digest():
    with raises(AssertionError, match="Wrong size:3"):
        Rake("abcd")

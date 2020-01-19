#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import getLogger

import pytest
from pytest import raises

from hashkernel.bakery import Cake, CakeTypes, MsgTypes

log = getLogger(__name__)


@pytest.mark.parametrize(
    "content, cake_s",
    [
        (b"", "RZwTDmWjELXeEmMEb0eIIegKayGGUPNsuJweEPhlXi50"),
        (b"a" * 1, "M2wFVNEmDX1opXFXKYMsbTv6wtlFLKv406fP1Io9PHd0"),
        (b"a" * 2, "zAThEaG91LzDkgXFISdnmebew615YIbWchH4Ds0mMnA0"),
        (b"a" * 3, "A5JIzO4SkX4P9iGsDnDIMHq0LuGUToKaOoQXCFIFCJq0"),
        (b"a" * 32, "e8Q7QjIGj9zzpWVkah5IE8nbGQwLpPycM22s743K6BV0"),
        (b"a" * 33, "vzEjZ0FMxLvbBgui5dOjjUqOXkozRdpndWdkd8GFEvM0"),
        (b"a" * 46, "ofsWs1MD2bqX34KrhZDGpw8I2LGyrDadV90nYzThzPt0"),
    ],
)
def test_cake_roundtrip(content, cake_s):
    u1 = Cake.from_bytes(content)
    assert cake_s == str(u1)
    u1n = Cake(str(u1))
    u1b = Cake(bytes(u1))
    assert u1.hash_key == u1n.hash_key
    assert u1 == u1n
    assert u1.hash_key == u1b.hash_key
    assert u1 == u1b


def test_guid():
    d = Cake.new_guid(MsgTypes.MOUNT_FOLDER)
    x = Cake.new_guid(MsgTypes.MOUNT_FOLDER)

    z = Cake(str(d))
    assert z == d
    assert (z != d) == False
    assert z != x
    assert z.type == d.type

    z = Cake(bytes(d))
    assert z == d
    assert (z != d) == False
    assert z != x
    assert z.type == d.type

    assert d != x
    assert str(z) == str(d)


def test_wrong_size_of_digest():
    with raises(AttributeError, match="digest is wrong size: 3 'abcd'"):
        Cake.from_hash_key("abcd", CakeTypes.NO_CLASS)

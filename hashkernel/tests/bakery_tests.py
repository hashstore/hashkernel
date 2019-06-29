#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import tempfile
from io import BytesIO
from logging import getLogger



import hashkernel.bakery as bakery
import hashkernel.bakery.cake as cake
from hashkernel import to_json, utf8_reader

log = getLogger(__name__)


def test_PatchAction():
    assert cake.PatchAction.update == cake.PatchAction["update"]
    assert cake.PatchAction.delete == cake.PatchAction["delete"]
    assert cake.PatchAction.delete == cake.PatchAction.ensure_it("delete")
    assert cake.PatchAction.update == cake.PatchAction.ensure_it_or_none("update")
    assert (cake.PatchAction.ensure_it_or_none(None) is None)
    assert str(cake.PatchAction.update) == "update"


def test_CAKe():
    def do_test(c, s, d=None):
        u1 = cake.Cake.from_bytes(c)
        assert s == str(u1)
        u1n = cake.Cake(str(u1))
        assert u1.digest() == u1n.digest()
        assert u1 == u1n
        if d is None:
            assert (not (u1.is_inlined))
        else:
            assert (u1.is_inlined)
            assert c == u1.data()

    do_test(b"", "0", True)
    do_test(b"a" * 1, "1z0", True)
    do_test(b"a" * 2, "6u50", True)
    do_test(b"a" * 3, "qMed0", True)
    do_test(b"a" * 32, "n5He1k77fjNxZNzBxGpha2giODrkmwQfOg6WorIJ4m50", True)
    do_test(b"a" * 33, "vzEjZ0FMxLvbBgui5dOjjUqOXkozRdpndWdkd8GFEvM1")

    do_test(b"a" * 46, "ofsWs1MD2bqX34KrhZDGpw8I2LGyrDadV90nYzThzPt1")

    b = bakery
    c = cake
    d = c.Cake.new_guid(c.CakeHeader.MOUNT_FOLDER)
    x = c.Cake.new_guid(c.CakeHeader.MOUNT_FOLDER)
    z = c.Cake(str(d))
    assert (z == d)
    assert (z != d) == False
    assert (z != x)
    assert (d != x)
    assert (z.header == d.header)
    assert (str(z) == str(d))


def test_Bundle():
    inline_udk = "1aMUQDApalaaYbXFjBVMMvyCAMfSPcTojI0745igi0"
    b1 = cake.CakeRack()
    assert b1.content() == "[[], []]"
    u1 = b1.cake()
    u0 = u1
    with tempfile.NamedTemporaryFile("w", delete=False) as w:
        w.write(b1.content())
    b2 = cake.CakeRack().parse(b1.content())
    u_f = cake.Cake.from_file(w.name, header=cake.CakeHeader.FOLDER)
    os.unlink(w.name)
    u2 = b2.cake()
    assert u_f == u2
    assert u1 == u2
    assert (u1 == u2)
    b1["a"] = inline_udk
    udk_bundle_str = '[["a"], ["%s"]]' % inline_udk
    assert str(b1) == udk_bundle_str
    u1 = b1.cake()
    assert (u1 != u2)
    b2.parse(utf8_reader(BytesIO(bytes(b1))))
    assert str(b2) == udk_bundle_str
    assert b2.size() == 55
    u2 = b2.cake()
    assert u1 == u2
    del b2["a"]
    u2 = b2.cake()
    assert u0 == u2
    assert b1["a"] == cake.Cake(inline_udk)
    assert b1.get_cakes() == [cake.Cake(inline_udk)]
    assert [k for k in b1] == ["a"]
    assert [k for k in b2] == []
    assert b1.get_name_by_cake(inline_udk) == "a"
    assert b1.get_name_by_cake(str(cake.Cake(inline_udk))) == "a"
    assert cake.CakeRack(to_json(b1)) == b1
    assert cake.CakeRack.ensure_it(to_json(b1)) == b1
    assert len(b1) == 1
    assert str(b1) == udk_bundle_str
    assert hash(b1) == hash(udk_bundle_str)
    assert (u1 == str(u1)) == False

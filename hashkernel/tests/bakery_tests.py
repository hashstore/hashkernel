#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashkernel.bakery as bakery
import hashkernel.bakery.cake as cake
from io import BytesIO
import os

from hashkernel import utf8_reader, to_json
from hs_build_tools.pytest import eq_, ok_
import tempfile

from logging import getLogger
log = getLogger(__name__)


def test_PatchAction():
    eq_(cake.PatchAction.update, cake.PatchAction['update'])
    eq_(cake.PatchAction.delete, cake.PatchAction['delete'])
    eq_(cake.PatchAction.delete, cake.PatchAction.ensure_it('delete'))
    eq_(cake.PatchAction.update, cake.PatchAction.ensure_it_or_none('update'))
    ok_(cake.PatchAction.ensure_it_or_none(None) is None)
    eq_(str(cake.PatchAction.update), 'update')


def test_CAKe():
    def do_test(c, s, d=None):
        u1 = cake.Cake.from_bytes(c)
        eq_(s, str(u1))
        u1n = cake.Cake(str(u1))
        eq_(u1.digest(), u1n.digest())
        eq_(u1, u1n)
        if d is None:
            ok_(not(u1.is_inlined))
        else:
            ok_(u1.is_inlined)
            eq_(c,u1.data())

    do_test(b'', '0', True)
    do_test(b'a' * 1, '01z', True)
    do_test(b'a' * 2, '06u5', True)
    do_test(b'a' * 3, '0qMed', True)
    do_test(b'a' * 32, '0n5He1k77fjNxZNzBxGpha2giODrkmwQfOg6WorIJ4m5',
            True)
    do_test(b'a' * 33, '1vzEjZ0FMxLvbBgui5dOjjUqOXkozRdpndWdkd8GFEvM')

    do_test(b'a' * 46, '1ofsWs1MD2bqX34KrhZDGpw8I2LGyrDadV90nYzThzPt')

    b = bakery
    c = cake
    d = c.Cake.new_guid(c.CakeHeader.MOUNT_FOLDER)
    x = c.Cake.new_guid(c.CakeHeader.MOUNT_FOLDER)
    z = c.Cake(str(d))
    ok_(z == d)
    eq_(z != d, False)
    ok_(z != x)
    ok_(d != x)
    ok_(z.header == d.header)
    ok_(str(z) == str(d))


def test_Bundle():
    inline_udk = '01aMUQDApalaaYbXFjBVMMvyCAMfSPcTojI0745igi'
    b1 = cake.CakeRack()
    eq_(b1.content(),'[[], []]')
    u1 = b1.cake()
    u0 = u1
    with tempfile.NamedTemporaryFile('w', delete=False) as w:
        w.write(b1.content())
    b2 = cake.CakeRack().parse(b1.content())
    u_f = cake.Cake.from_file(w.name, header=cake.CakeHeader.FOLDER)
    os.unlink(w.name)
    u2 = b2.cake()
    eq_(u_f, u2)
    eq_(u1,u2)
    ok_(u1 == u2)
    b1['a'] = inline_udk
    udk_bundle_str = '[["a"], ["%s"]]' % inline_udk
    eq_(str(b1), udk_bundle_str)
    u1 = b1.cake()
    ok_(u1 != u2)
    b2.parse(utf8_reader(BytesIO(bytes(b1))))
    eq_(str(b2), udk_bundle_str)
    eq_(b2.size(),55)
    u2 = b2.cake()
    eq_(u1, u2)
    del b2['a']
    u2= b2.cake()
    eq_(u0,u2)
    eq_(b1['a'], cake.Cake(inline_udk))
    eq_(b1.get_cakes(), [cake.Cake(inline_udk)])
    eq_([k for k in b1], ['a'])
    eq_([k for k in b2], [])
    eq_(b1.get_name_by_cake(inline_udk), 'a')
    eq_(b1.get_name_by_cake(str(cake.Cake(inline_udk))), 'a')
    eq_(cake.CakeRack(to_json(b1)), b1)
    eq_(cake.CakeRack.ensure_it(to_json(b1)), b1)
    eq_(len(b1),1)
    eq_(str(b1),udk_bundle_str)
    eq_(hash(b1),hash(udk_bundle_str))
    eq_(u1 == str(u1), False)







#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hs_build_tools import LogTestOut

from hashkernel.bakery.cask import Caskade

log, out = LogTestOut.get(__name__)

caskade = Caskade(out.child_dir("caskade"))


def test_cask():

    assert True

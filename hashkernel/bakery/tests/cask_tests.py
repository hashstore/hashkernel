#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pathlib import Path

import pytest
from hs_build_tools import LogTestOut

from hashkernel.bakery import NULL_CAKE
from hashkernel.bakery.cask import CHUNK_SIZE, Caskade, CaskadeConfig
from hashkernel.bakery.tests import BytesGen
from hashkernel.time import FEW_SECONDS_TTL

log, out = LogTestOut.get(__name__)

caskades = Path(out.child_dir("caskades"))

common_config = CaskadeConfig(
    origin=NULL_CAKE,
    checkpoint_ttl=FEW_SECONDS_TTL,
    checkpoint_size=8 * CHUNK_SIZE,
    max_cask_size=20 * CHUNK_SIZE,
)


@pytest.mark.parametrize(
    "name, config", [("config_none", None), ("common", common_config)]
)
def test_config(name, config):
    new_ck = Caskade(caskades / name, config=config)
    loaded_ck = Caskade(new_ck.dir)
    assert new_ck.config == loaded_ck.config
    assert type(new_ck.config.checkpoint_ttl) == type(loaded_ck.config.checkpoint_ttl)


def test_3steps():
    caskade = Caskade(caskades / "3steps", config=common_config)
    bg = BytesGen(0)
    caskade.write_bytes(bg.get_bytes((CHUNK_SIZE * 5) // 4))

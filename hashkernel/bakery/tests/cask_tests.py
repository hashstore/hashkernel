#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pathlib import Path

import pytest
from hs_build_tools import LogTestOut
from nanotime import nanotime

from hashkernel.bakery import NULL_CAKE
from hashkernel.bakery.cask import (
    CHUNK_SIZE,
    Caskade,
    CaskadeConfig,
    CheckpointEntry,
    CheckPointType,
    EntryType,
    Record,
    Record_PACKER,
)
from hashkernel.tests import BytesGen
from hashkernel.time import FEW_SECONDS_TTL

log, out = LogTestOut.get(__name__)

caskades = Path(out.child_dir("caskades"))

common_config = CaskadeConfig(
    origin=NULL_CAKE,
    checkpoint_ttl=FEW_SECONDS_TTL,
    checkpoint_size=8 * CHUNK_SIZE,
    max_cask_size=20 * CHUNK_SIZE,
)


def test_entries():
    r = Record(EntryType.DATA, nanotime(0), NULL_CAKE)
    pack = Record_PACKER.pack(r)
    r2, offset = Record_PACKER.unpack(pack, 0)
    assert len(pack) == offset
    assert r.entry_type == r2.entry_type
    assert r.tstamp.nanoseconds() == r2.tstamp.nanoseconds()
    assert r.src == r2.src
    packer = EntryType.CHECK_POINT.entry_packer
    o = CheckpointEntry(0, 5, CheckPointType.ON_SIZE)
    pack = packer.pack(o)
    o2, offset = packer.unpack(pack, 0)
    assert len(pack) == offset
    assert o == o2


@pytest.mark.parametrize(
    "name, config", [("config_none", None), ("common", common_config)]
)
def test_config(name, config):
    new_ck = Caskade(caskades / name, config=config)
    loaded_ck = Caskade(new_ck.dir)

    assert new_ck.config == loaded_ck.config
    assert type(new_ck.config.checkpoint_ttl) == type(loaded_ck.config.checkpoint_ttl)


@pytest.mark.slow
def test_3steps():
    caskade = Caskade(caskades / "3steps", config=common_config)
    bg = BytesGen(0)
    cake = caskade.write_bytes(bg.get_bytes((CHUNK_SIZE * 5) // 4))
    read_caskade = Caskade(caskades / "3steps")
    rdp = read_caskade.data_locations[cake]
    dp = caskade.data_locations[cake]
    assert rdp == dp
    assert rdp.offset == dp.offset
    assert rdp.size == dp.size

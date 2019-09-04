#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pathlib import Path
from time import sleep

import pytest
from hs_build_tools import LogTestOut
from nanotime import nanotime

from hashkernel.bakery import NULL_CAKE, CakeTypes, Cake
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
from hashkernel.packer import SIZED_BYTES
from hashkernel.tests import rand_bytes
from hashkernel.time import TTL

log, out = LogTestOut.get(__name__)

caskades = Path(out.child_dir("caskades"))

common_config = CaskadeConfig(
    origin=NULL_CAKE,
    checkpoint_ttl=TTL(1),
    checkpoint_size=8 * CHUNK_SIZE,
    max_cask_size=11 * CHUNK_SIZE,
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


ONE_AND_QUARTER = (CHUNK_SIZE * 5) // 4
ABOUT_HALF = 1 + CHUNK_SIZE // 2
TWOTHIRD_OF_CHUNK = (2 * CHUNK_SIZE) // 3


@pytest.mark.slow
def test_3steps():
    caskade = Caskade(caskades / "3steps", config=common_config)
    header_size = Record_PACKER.size + EntryType.CASK_HEADER.entry_packer.size
    cp_size = Record_PACKER.size + EntryType.CHECK_POINT.size
    first_cask = caskade.active.guid
    assert caskade.active.tracker.current_offset == header_size
    a0 = caskade.write_bytes(rand_bytes(0, ONE_AND_QUARTER))
    assert first_cask == caskade.active.guid
    p = header_size

    def adjust_p(data_size):
        nonlocal p
        p = (
            p
            + data_size
            + Record_PACKER.size
            + len(SIZED_BYTES.size_packer.pack(data_size))
        )

    adjust_p(ONE_AND_QUARTER)
    assert first_cask == caskade.active.guid
    assert caskade.active.tracker.current_offset == p
    h0 = caskade.write_bytes(rand_bytes(0, ABOUT_HALF))
    adjust_p(ABOUT_HALF)
    assert caskade.active.tracker.current_offset == p
    a1 = caskade.write_bytes(rand_bytes(1, ONE_AND_QUARTER))
    adjust_p(ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == p
    a2 = caskade.write_bytes(rand_bytes(2, ONE_AND_QUARTER))
    adjust_p(ONE_AND_QUARTER)
    assert first_cask == caskade.active.guid
    assert caskade.active.tracker.current_offset == p
    a3 = caskade.write_bytes(rand_bytes(3, ONE_AND_QUARTER))
    adjust_p(ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == p
    a4 = caskade.write_bytes(rand_bytes(4, ONE_AND_QUARTER))
    adjust_p(ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == p
    a5 = caskade.write_bytes(rand_bytes(5, ONE_AND_QUARTER))
    adjust_p(ONE_AND_QUARTER)
    # cp1 by size
    p = p + cp_size
    assert caskade.active.tracker.current_offset == p
    assert first_cask == caskade.active.guid

    h1 = caskade.write_bytes(rand_bytes(1, ABOUT_HALF))
    adjust_p(ABOUT_HALF)
    assert caskade.active.tracker.current_offset == p
    sleep(20)
    h2 = caskade.write_bytes(rand_bytes(2, ABOUT_HALF))
    adjust_p(ABOUT_HALF)
    # cp2 by time
    p = p + Record_PACKER.size + EntryType.CHECK_POINT.size
    assert caskade.active.tracker.current_offset == p
    assert first_cask == caskade.active.guid

    h3 = caskade.write_bytes(rand_bytes(3, ABOUT_HALF))
    adjust_p(ABOUT_HALF)
    assert caskade.active.tracker.current_offset == p
    h4 = caskade.write_bytes(rand_bytes(4, ABOUT_HALF))
    adjust_p(ABOUT_HALF)
    assert caskade.active.tracker.current_offset == p
    a6 = caskade.write_bytes(rand_bytes(6, ONE_AND_QUARTER))
    # new_cask
    assert first_cask != caskade.active.guid
    p = header_size
    adjust_p(ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == p
    a1_again = caskade.write_bytes(rand_bytes(1, ONE_AND_QUARTER))
    assert a1 == a1_again
    assert caskade.active.tracker.current_offset == p

    read_caskade = Caskade(caskades / "3steps")

    assert read_caskade.data_locations.keys() == caskade.data_locations.keys()
    for k in read_caskade.data_locations.keys():
        rdp = read_caskade.data_locations[k]
        dp = caskade.data_locations[k]
        assert rdp.offset == dp.offset
        assert rdp.size == dp.size
        assert k == Cake.from_bytes(read_caskade[k], CakeTypes.NO_CLASS)
        assert k == Cake.from_bytes(caskade[k], CakeTypes.NO_CLASS)



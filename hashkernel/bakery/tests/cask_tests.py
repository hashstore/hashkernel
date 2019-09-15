#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pathlib import Path
from time import sleep

import pytest
from hs_build_tools import LogTestOut
from nanotime import nanotime

from hashkernel.bakery import NULL_CAKE, Cake, CakeTypes
from hashkernel.bakery.cask import (
    CHECK_POINT_SIZE,
    CHUNK_SIZE,
    AccessError,
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
TINY = 1025
TWO_K = 2048

HEADER_SIZE = Record_PACKER.size + EntryType.CASK_HEADER.entry_packer.size
END_SEQ_SIZE = (
    Record_PACKER.size * 2 + EntryType.CHECK_POINT.size + EntryType.NEXT_CASK.size
)


class SizePredictor:
    def __init__(self):
        self.pos = HEADER_SIZE

    def add(self, size):
        self.pos = self.pos + size

    def add_data(self, data_size):
        self.add(
            data_size
            + Record_PACKER.size
            + len(SIZED_BYTES.size_packer.pack(data_size))
        )


def test_recover_no_checkpoints():
    caskade = Caskade(caskades / "recover_no_cp", config=common_config)
    sp = SizePredictor()
    first_cask = caskade.active.guid
    assert caskade.active.tracker.current_offset == HEADER_SIZE
    a0 = caskade.write_bytes(rand_bytes(0, TWO_K))
    assert first_cask == caskade.active.guid

    sp.add_data(TWO_K)
    assert first_cask == caskade.active.guid
    assert caskade.active.tracker.current_offset == sp.pos
    h0 = caskade.write_bytes(rand_bytes(0, TINY))
    sp.add_data(TINY)
    assert caskade.active.tracker.current_offset == sp.pos
    a1 = caskade.write_bytes(rand_bytes(1, TWO_K))
    sp.add_data(TWO_K)
    assert caskade.active.tracker.current_offset == sp.pos
    a1_again = caskade.write_bytes(rand_bytes(1, TWO_K))
    assert a1 == a1_again
    assert caskade.active.tracker.current_offset == sp.pos
    assert len(caskade.active) == sp.pos

    write_caskade = Caskade(caskades / "recover_no_cp")
    write_caskade.recover()
    sp.add(CHECK_POINT_SIZE)
    assert write_caskade.active.tracker.current_offset == sp.pos
    a2 = write_caskade.write_bytes(rand_bytes(2, TWO_K))
    sp.add_data(TWO_K)
    assert write_caskade.active.tracker.current_offset == sp.pos

    assert write_caskade.active.tracker.current_offset == sp.pos
    a1_again = caskade.write_bytes(rand_bytes(1, TWO_K))
    assert a1 == a1_again
    last_cask = write_caskade.active.guid
    write_caskade.close()
    sp.add(END_SEQ_SIZE)
    assert len(write_caskade.casks[last_cask]) == sp.pos


@pytest.mark.slow
def test_3steps():
    dir = caskades / "3steps"
    caskade = Caskade(dir, config=common_config)
    first_cask = caskade.active.guid
    assert caskade.active.tracker.current_offset == HEADER_SIZE
    a0 = caskade.write_bytes(rand_bytes(0, ONE_AND_QUARTER))
    assert first_cask == caskade.active.guid
    sp = SizePredictor()
    sp.add_data(ONE_AND_QUARTER)
    assert first_cask == caskade.active.guid
    assert caskade.active.tracker.current_offset == sp.pos
    h0 = caskade.write_bytes(rand_bytes(0, ABOUT_HALF))
    sp.add_data(ABOUT_HALF)
    assert caskade.active.tracker.current_offset == sp.pos
    a1 = caskade.write_bytes(rand_bytes(1, ONE_AND_QUARTER))
    sp.add_data(ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == sp.pos
    a2 = caskade.write_bytes(rand_bytes(2, ONE_AND_QUARTER))
    sp.add_data(ONE_AND_QUARTER)
    assert first_cask == caskade.active.guid
    assert caskade.active.tracker.current_offset == sp.pos
    a3 = caskade.write_bytes(rand_bytes(3, ONE_AND_QUARTER))
    sp.add_data(ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == sp.pos
    a4 = caskade.write_bytes(rand_bytes(4, ONE_AND_QUARTER))
    sp.add_data(ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == sp.pos
    a5 = caskade.write_bytes(rand_bytes(5, ONE_AND_QUARTER))
    sp.add_data(ONE_AND_QUARTER)
    # cp1 by size
    sp.add(CHECK_POINT_SIZE)
    assert caskade.active.tracker.current_offset == sp.pos
    assert first_cask == caskade.active.guid

    h1 = caskade.write_bytes(rand_bytes(1, ABOUT_HALF))
    sp.add_data(ABOUT_HALF)
    assert caskade.active.tracker.current_offset == sp.pos
    sleep(20)
    h2 = caskade.write_bytes(rand_bytes(2, ABOUT_HALF))
    sp.add_data(ABOUT_HALF)
    # cp2 by time
    sp.add(CHECK_POINT_SIZE)
    assert caskade.active.tracker.current_offset == sp.pos
    assert first_cask == caskade.active.guid

    h3 = caskade.write_bytes(rand_bytes(3, ABOUT_HALF))
    sp.add_data(ABOUT_HALF)
    assert caskade.active.tracker.current_offset == sp.pos
    h4 = caskade.write_bytes(rand_bytes(4, ABOUT_HALF))
    sp.add_data(ABOUT_HALF)
    assert caskade.active.tracker.current_offset == sp.pos
    a6 = caskade.write_bytes(rand_bytes(6, ONE_AND_QUARTER))
    # new_cask
    assert first_cask != caskade.active.guid
    sp = SizePredictor()
    sp.add_data(ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == sp.pos
    a1_again = caskade.write_bytes(rand_bytes(1, ONE_AND_QUARTER))
    assert a1 == a1_again
    assert caskade.active.tracker.current_offset == sp.pos
    idx = 0

    def logit(s):
        nonlocal idx
        (dir / f"{idx:03d}_{s}").write_bytes(b"")
        print(s)
        idx += 1

    # logit("read_caskade")
    read_caskade = Caskade(dir)

    assert read_caskade.data_locations.keys() == caskade.data_locations.keys()
    # logit("keys_match")
    for k in read_caskade.data_locations.keys():
        rdp = read_caskade.data_locations[k]
        dp = caskade.data_locations[k]
        assert rdp.offset == dp.offset
        assert rdp.size == dp.size
        assert k == Cake.from_bytes(read_caskade[k], CakeTypes.NO_CLASS)
        assert k == Cake.from_bytes(caskade[k], CakeTypes.NO_CLASS)
        # logit(str(k)[:8])

    # logit("all_matched")

    caskade.pause()
    # logit("pause")

    sp.add(CHECK_POINT_SIZE)

    write_caskade = Caskade(dir)
    assert write_caskade.check_points[0].type == CheckPointType.ON_CASK_HEADER

    assert caskade.check_points == write_caskade.check_points
    assert write_caskade.check_points[-1].type == CheckPointType.ON_CASKADE_PAUSE
    # logit("write_caskade")

    write_caskade.resume()
    # logit("resume")
    sp.add(CHECK_POINT_SIZE)
    assert write_caskade.active.tracker.current_offset == sp.pos

    assert write_caskade.check_points[-1].type == CheckPointType.ON_CASKADE_RESUME
    a7 = write_caskade.write_bytes(rand_bytes(7, ONE_AND_QUARTER))
    sp.add_data(ONE_AND_QUARTER)
    assert write_caskade.active.tracker.current_offset == sp.pos
    # logit("write_more")

    a1_again = write_caskade.write_bytes(rand_bytes(1, ONE_AND_QUARTER))
    assert a1 == a1_again
    assert write_caskade.active.tracker.current_offset == sp.pos

    # logit("abandon")

    write_caskade = Caskade(dir)
    write_caskade.recover(1)
    # logit("recover_1")

    a8 = write_caskade.write_bytes(rand_bytes(8, ONE_AND_QUARTER))

    write_caskade.close()
    # logit("close")

    with pytest.raises(AccessError):
        write_caskade.write_bytes(rand_bytes(9, ONE_AND_QUARTER))

    with pytest.raises(AccessError):
        write_caskade.recover(0)

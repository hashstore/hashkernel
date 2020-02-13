#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pathlib import Path
from time import sleep

import pytest
from hs_build_tools import LogTestOut
from nanotime import nanotime

from hashkernel.bakery import NULL_CAKE, Cake, CakeTypes
from hashkernel.caskade import (
    CHUNK_SIZE,
    AccessError,
    BaseEntries,
    CaskadeConfig,
    CaskHashSigner,
    CheckpointHeader,
    CheckPointType,
    Record,
    Record_PACKER,
)
from hashkernel.caskade.cask import BaseCaskade, Caskade
from hashkernel.caskade.optional import OptionalCaskade, OptionalEntries, Tag
from hashkernel.hashing import HashKey
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

common_singer = CaskadeConfig(
    origin=NULL_CAKE,
    checkpoint_ttl=TTL(1),
    checkpoint_size=8 * CHUNK_SIZE,
    max_cask_size=11 * CHUNK_SIZE,
    signer=CaskHashSigner(),
)


def test_entries():
    r = Record(BaseEntries.DATA.code, nanotime(0))
    pack = Record_PACKER.pack(r)
    r2, offset = Record_PACKER.unpack(pack, 0)
    assert len(pack) == offset
    assert r.entry_code == r2.entry_code
    assert r.tstamp.nanoseconds() == r2.tstamp.nanoseconds()
    packer = BaseEntries.CHECK_POINT.header_packer
    o = CheckpointHeader(NULL_CAKE.hash_key, 0, 5, CheckPointType.ON_SIZE)
    pack = packer.pack(o)
    o2, offset = packer.unpack(pack, 0)
    assert len(pack) == offset
    assert o == o2


@pytest.mark.parametrize(
    "name, entry_types, config",
    [
        ("config_none", BaseEntries, None),
        ("common", BaseEntries, common_config),
        ("singer", BaseEntries, common_singer),
    ],
)
def test_config(name, entry_types, config):
    new_ck = Caskade(caskades / name, entry_types=entry_types, config=config)
    loaded_ck = Caskade(new_ck.meta.dir, entry_types)

    assert new_ck.meta.config == loaded_ck.meta.config
    assert new_ck.meta.config.signature_size() == loaded_ck.meta.config.signature_size()
    assert type(new_ck.meta.config.checkpoint_ttl) == type(
        loaded_ck.meta.config.checkpoint_ttl
    )


ONE_AND_QUARTER = (CHUNK_SIZE * 5) // 4
ABOUT_HALF = 1 + CHUNK_SIZE // 2
TWOTHIRD_OF_CHUNK = (2 * CHUNK_SIZE) // 3
TINY = 1025
TWO_K = 2048


class SizePredictor:
    def __init__(self, pos=0):
        self.pos = pos

    def add(self, size):
        self.pos = self.pos + size

    def add_data(self, caskade, data_size):
        self.add(
            caskade.meta.size_of_entry(BaseEntries.DATA,  data_size)
        )


def test_recover_no_checkpoints():
    caskade = Caskade(
        caskades / "recover_no_cp", entry_types=BaseEntries, config=common_config
    )
    sp = SizePredictor(caskade.meta.size_of_header())
    first_cask = caskade.active.guid
    assert caskade.active.tracker.current_offset == caskade.meta.size_of_header()
    a0 = caskade.write_bytes(rand_bytes(0, TWO_K))
    assert first_cask == caskade.active.guid

    sp.add_data(caskade,TWO_K)
    assert first_cask == caskade.active.guid
    assert caskade.active.tracker.current_offset == sp.pos
    h0 = caskade.write_bytes(rand_bytes(0, TINY))
    sp.add_data(caskade, TINY)
    assert caskade.active.tracker.current_offset == sp.pos
    a1 = caskade.write_bytes(rand_bytes(1, TWO_K))
    sp.add_data(caskade, TWO_K)
    assert caskade.active.tracker.current_offset == sp.pos
    a1_again = caskade.write_bytes(rand_bytes(1, TWO_K))
    assert a1 == a1_again
    assert caskade.active.tracker.current_offset == sp.pos
    assert len(caskade.active) == sp.pos

    write_caskade = Caskade(caskades / "recover_no_cp", BaseEntries)
    write_caskade.recover()
    sp.add(write_caskade.meta.size_of_checkpoint())
    assert write_caskade.active.tracker.current_offset == sp.pos
    a2 = write_caskade.write_bytes(rand_bytes(2, TWO_K))
    sp.add_data(write_caskade, TWO_K)
    assert write_caskade.active.tracker.current_offset == sp.pos

    assert write_caskade.active.tracker.current_offset == sp.pos
    a1_again = caskade.write_bytes(rand_bytes(1, TWO_K))
    assert a1 == a1_again
    last_cask = write_caskade.active.guid
    write_caskade.close()
    sp.add(write_caskade.meta.size_of_end_sequence())
    assert len(write_caskade.casks[last_cask]) == sp.pos


@pytest.mark.slow
@pytest.mark.parametrize(
    "name, caskade_cls, config",
    [
        ("common", BaseCaskade, common_config),
        ("singer", BaseCaskade, common_singer),
        ("common_opt", OptionalCaskade, common_config),
        ("singer_opt", OptionalCaskade, common_singer),
    ],
)
def test_3steps(name, caskade_cls, config):
    dir = caskades / f"3steps_{name}"
    caskade = caskade_cls(dir, config=config)
    print(caskade.meta.size_of_checkpoint())
    first_cask = caskade.active.guid
    assert caskade.active.tracker.current_offset == caskade.meta.size_of_header()
    a0 = caskade.write_bytes(rand_bytes(0, ONE_AND_QUARTER))
    assert first_cask == caskade.active.guid
    sp = SizePredictor(caskade.meta.size_of_header())
    sp.add_data(caskade,ONE_AND_QUARTER)
    assert first_cask == caskade.active.guid
    assert caskade.active.tracker.current_offset == sp.pos
    h0 = caskade.write_bytes(rand_bytes(0, ABOUT_HALF))
    sp.add_data(caskade,ABOUT_HALF)
    assert caskade.active.tracker.current_offset == sp.pos
    a1 = caskade.write_bytes(rand_bytes(1, ONE_AND_QUARTER))
    sp.add_data(caskade, ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == sp.pos
    a2 = caskade.write_bytes(rand_bytes(2, ONE_AND_QUARTER))
    sp.add_data(caskade, ONE_AND_QUARTER)
    assert first_cask == caskade.active.guid
    assert caskade.active.tracker.current_offset == sp.pos
    a3 = caskade.write_bytes(rand_bytes(3, ONE_AND_QUARTER))
    sp.add_data(caskade, ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == sp.pos
    a4_bytes = rand_bytes(4, ONE_AND_QUARTER)
    a4 = caskade.write_bytes(a4_bytes)
    sp.add_data(caskade, ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == sp.pos

    a4_permalink = Cake.new_guid()
    caskade.set_link(a4_permalink, 0, a4)
    sp.add(caskade.meta.size_of_entry(BaseEntries.LINK))

    a4_derived = HashKey.from_bytes(a4_bytes[:100])
    a4_tag = Tag(name="Hello")
    if caskade_cls == OptionalCaskade:
        caskade.save_derived(a4, a4_permalink, a4_derived)
        sp.add(caskade.meta.size_of_entry(OptionalEntries.DERIVED))

        caskade.tag(a4_permalink, a4_tag)
        sp.add(caskade.meta.size_of_entry(OptionalEntries.TAG, len(bytes(a4_tag))))

    a5 = caskade.write_bytes(rand_bytes(5, ONE_AND_QUARTER))
    sp.add_data(caskade, ONE_AND_QUARTER)

    # cp1 by size
    sp.add(caskade.meta.size_of_checkpoint())
    assert caskade.active.tracker.current_offset == sp.pos
    assert first_cask == caskade.active.guid

    h1 = caskade.write_bytes(rand_bytes(1, ABOUT_HALF))
    sp.add_data(caskade, ABOUT_HALF)
    assert caskade.active.tracker.current_offset == sp.pos
    sleep(20)
    h2 = caskade.write_bytes(rand_bytes(2, ABOUT_HALF))
    sp.add_data(caskade, ABOUT_HALF)
    # cp2 by time
    sp.add(caskade.meta.size_of_checkpoint())
    assert caskade.active.tracker.current_offset == sp.pos
    assert first_cask == caskade.active.guid

    h3 = caskade.write_bytes(rand_bytes(3, ABOUT_HALF))
    sp.add_data(caskade, ABOUT_HALF)
    assert caskade.active.tracker.current_offset == sp.pos
    h4 = caskade.write_bytes(rand_bytes(4, ABOUT_HALF))
    sp.add_data(caskade, ABOUT_HALF)
    assert caskade.active.tracker.current_offset == sp.pos
    a6 = caskade.write_bytes(rand_bytes(6, ONE_AND_QUARTER))
    # new_cask
    assert first_cask != caskade.active.guid
    sp = SizePredictor(caskade.meta.size_of_header())
    sp.add_data(caskade,ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == sp.pos
    a1_again = caskade.write_bytes(rand_bytes(1, ONE_AND_QUARTER))
    assert a1 == a1_again
    assert caskade.active.tracker.current_offset == sp.pos
    # idx = 0
    #
    # def logit(s):
    #     nonlocal idx
    #     (dir / f"{idx:03d}_{s}").write_bytes(b"")
    #     print(s)
    #     idx += 1

    # logit("read_caskade")
    read_caskade = caskade_cls(dir)

    assert read_caskade.data_locations.keys() == caskade.data_locations.keys()
    # logit("keys_match")
    for k in read_caskade.data_locations.keys():
        rdp = read_caskade.data_locations[k]
        dp = caskade.data_locations[k]
        assert rdp.offset == dp.offset
        assert rdp.size == dp.size
        assert k == HashKey.from_bytes(read_caskade[k])
        assert k == HashKey.from_bytes(caskade[k])
        # logit(str(k)[:8])
    if caskade_cls == OptionalCaskade:
        assert read_caskade.derived[a4][a4_permalink]==a4_derived
        assert caskade.tags[a4_permalink][0] == a4_tag

    assert read_caskade.datalinks[a4_permalink][0] == a4
    # assert read_caskade.tags[a4] == [a4_tag]
    # assert read_caskade.derived[a4][a4_permalink] == a4_derived

    # logit("all_matched")

    caskade.pause()
    # logit("pause")

    sp.add(caskade.meta.size_of_checkpoint())

    write_caskade = caskade_cls(dir)
    assert write_caskade.check_points[0].type == CheckPointType.ON_CASK_HEADER

    assert caskade.check_points == write_caskade.check_points
    assert write_caskade.check_points[-1].type == CheckPointType.ON_CASKADE_PAUSE
    # logit("write_caskade")

    write_caskade.resume()
    # logit("resume")
    sp.add(write_caskade.meta.size_of_checkpoint())
    assert write_caskade.active.tracker.current_offset == sp.pos

    assert write_caskade.check_points[-1].type == CheckPointType.ON_CASKADE_RESUME
    a7 = write_caskade.write_bytes(rand_bytes(7, ONE_AND_QUARTER))
    sp.add_data(write_caskade, ONE_AND_QUARTER)
    assert write_caskade.active.tracker.current_offset == sp.pos
    # logit("write_more")

    a1_again = write_caskade.write_bytes(rand_bytes(1, ONE_AND_QUARTER))
    assert a1 == a1_again
    assert write_caskade.active.tracker.current_offset == sp.pos

    # logit("abandon")

    write_caskade = caskade_cls(dir)
    write_caskade.recover(1)
    # logit("recover_1")

    a8 = write_caskade.write_bytes(rand_bytes(8, ONE_AND_QUARTER))

    write_caskade.close()
    # logit("close")

    with pytest.raises(AccessError):
        write_caskade.write_bytes(rand_bytes(9, ONE_AND_QUARTER))

    with pytest.raises(AccessError):
        write_caskade.recover(0)

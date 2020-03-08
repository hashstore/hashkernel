#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pathlib import Path
from time import sleep, time

import pytest
from hs_build_tools import LogTestOut
from nanotime import nanotime

from hashkernel.bakery import NULL_CAKE, Cake, CakeTypes
from hashkernel.caskade import (
    CHUNK_SIZE,
    AccessError,
    BaseJots,
    CaskadeConfig,
    CaskHashSigner,
    Catalog_PACKER,
    CheckpointHeader,
    CheckPointType,
    Stamp,
    Stamp_PACKER,
)
from hashkernel.caskade.cask import (
    BaseCaskade,
    Caskade,
    size_of_check_point,
    size_of_entry,
)
from hashkernel.caskade.optional import OptionalCaskade, OptionalJots, Tag
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


def test_packers():
    r = Stamp(BaseJots.DATA.code, nanotime(0))
    pack = Stamp_PACKER.pack(r)
    r2, offset = Stamp_PACKER.unpack(pack, 0)
    assert len(pack) == offset
    assert r.entry_code == r2.entry_code
    assert r.tstamp.nanoseconds() == r2.tstamp.nanoseconds()
    packer = BaseJots.CHECK_POINT.header_packer
    o = CheckpointHeader(NULL_CAKE.hash_key, 0, 5, CheckPointType.ON_SIZE)
    pack = packer.pack(o)
    o2, offset = packer.unpack(pack, 0)
    assert len(pack) == offset
    assert o == o2


@pytest.mark.parametrize(
    "entries, conform_to", [(BaseJots, OptionalJots), (OptionalJots, BaseJots)]
)
def test_catalog(entries, conform_to):
    cat = entries.catalog()
    pack = Catalog_PACKER.pack(cat)
    cat2, end = Catalog_PACKER.unpack(pack, 0)
    assert len(pack) == end
    assert cat == cat2
    new_entries, _ = entries.force_in(conform_to.catalog(), expand=True)
    if entries == OptionalJots:
        assert new_entries == entries
    else:
        assert new_entries.catalog() == conform_to.catalog()


@pytest.mark.parametrize(
    "name, jot_types, config",
    [
        ("config_none", BaseJots, None),
        ("common", BaseJots, common_config),
        ("singer", BaseJots, common_singer),
    ],
)
def test_config(name, jot_types, config):
    new_ck = Caskade(caskades / name, jot_types=jot_types, config=config)
    loaded_ck = Caskade(new_ck.dir, jot_types)

    assert new_ck.config == loaded_ck.config
    assert new_ck.config.signature_size() == loaded_ck.config.signature_size()
    assert type(new_ck.config.checkpoint_ttl) == type(loaded_ck.config.checkpoint_ttl)


ONE_AND_QUARTER = (CHUNK_SIZE * 5) // 4
ABOUT_HALF = 1 + CHUNK_SIZE // 2
TWOTHIRD_OF_CHUNK = (2 * CHUNK_SIZE) // 3
TINY = 1025
TWO_K = 2048


class SizePredictor:
    def __init__(self, caskade: Caskade):
        self.cascade = caskade
        self.pos = size_of_entry(
            BaseJots.CASK_HEADER, len(self.cascade.latest_file().catalog.binary)
        )

    def add(self, size):
        self.pos = self.pos + size

    def add_data(self, data_size):
        self.add(size_of_entry(BaseJots.DATA, data_size))

    def add_check_point(self):
        self.add(size_of_check_point(self.cascade))

    def add_end_sequence(self):
        self.add(size_of_entry(BaseJots.NEXT_CASK))
        self.add_check_point()


def test_recover_no_checkpoints():
    caskade = Caskade(
        caskades / "recover_no_cp", jot_types=BaseJots, config=common_config
    )
    sp = SizePredictor(caskade)
    first_cask = caskade.active.guid
    assert caskade.active.tracker.current_offset == sp.pos
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

    write_caskade = Caskade(caskades / "recover_no_cp", BaseJots)
    write_caskade.recover()
    sp.add_check_point()
    assert write_caskade.active.tracker.current_offset == sp.pos
    a2 = write_caskade.write_bytes(rand_bytes(2, TWO_K))
    sp.add_data(TWO_K)
    assert write_caskade.active.tracker.current_offset == sp.pos

    assert write_caskade.active.tracker.current_offset == sp.pos
    a1_again = caskade.write_bytes(rand_bytes(1, TWO_K))
    assert a1 == a1_again
    last_cask = write_caskade.active.guid
    write_caskade.close()
    sp.add_end_sequence()
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
    t = time()
    caskade = caskade_cls(dir, config=config)
    t = time()
    sp = SizePredictor(caskade)
    first_cask = caskade.active.guid
    assert caskade.active.tracker.current_offset == sp.pos
    a0 = caskade.write_bytes(rand_bytes(0, ONE_AND_QUARTER))
    assert first_cask == caskade.active.guid
    sp.add_data(ONE_AND_QUARTER)
    print(time() - t)
    assert first_cask == caskade.active.guid
    assert caskade.active.tracker.current_offset == sp.pos
    h0 = caskade.write_bytes(rand_bytes(0, ABOUT_HALF))
    sp.add_data(ABOUT_HALF)
    assert caskade.active.tracker.current_offset == sp.pos
    a1 = caskade.write_bytes(rand_bytes(1, ONE_AND_QUARTER))
    sp.add_data(ONE_AND_QUARTER)
    print(time() - t)
    assert caskade.active.tracker.current_offset == sp.pos
    a2 = caskade.write_bytes(rand_bytes(2, ONE_AND_QUARTER))
    sp.add_data(ONE_AND_QUARTER)
    assert first_cask == caskade.active.guid
    assert caskade.active.tracker.current_offset == sp.pos
    a3 = caskade.write_bytes(rand_bytes(3, ONE_AND_QUARTER))
    print(time() - t)
    sp.add_data(ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == sp.pos
    a4_bytes = rand_bytes(4, ONE_AND_QUARTER)
    a4 = caskade.write_bytes(a4_bytes)
    sp.add_data(ONE_AND_QUARTER)
    assert caskade.active.tracker.current_offset == sp.pos
    print(time() - t)

    a4_permalink = Cake.new_guid()
    caskade.set_link(a4_permalink, 0, a4)
    sp.add(size_of_entry(BaseJots.LINK))

    a4_derived = HashKey.from_bytes(a4_bytes[:100])
    a4_tag = Tag(name="Hello")
    if caskade_cls == OptionalCaskade:
        caskade.save_derived(a4, a4_permalink, a4_derived)
        sp.add(size_of_entry(OptionalJots.DERIVED))
        print(time() - t)

        caskade.tag(a4_permalink, a4_tag)
        sp.add(size_of_entry(OptionalJots.TAG, len(bytes(a4_tag))))
    print(time() - t)

    a5 = caskade.write_bytes(rand_bytes(5, ONE_AND_QUARTER))
    sp.add_data(ONE_AND_QUARTER)
    print(time() - t)

    # cp1 by size
    sp.add_check_point()
    t = time()
    assert caskade.active.tracker.current_offset == sp.pos
    assert first_cask == caskade.active.guid
    print(time() - t)

    h1 = caskade.write_bytes(rand_bytes(1, ABOUT_HALF))
    sp.add_data(ABOUT_HALF)
    print(time() - t)
    assert caskade.active.tracker.current_offset == sp.pos
    sleep(21)
    print(time() - t)
    h2 = caskade.write_bytes(rand_bytes(2, ABOUT_HALF))
    sp.add_data(ABOUT_HALF)
    # cp2 by time
    sp.add_check_point()
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
    sp = SizePredictor(caskade)
    sp.add_data(ONE_AND_QUARTER)
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
        assert read_caskade.derived[a4][a4_permalink] == a4_derived
        assert caskade.tags[a4_permalink][0] == a4_tag

    assert read_caskade.datalinks[a4_permalink][0] == a4
    # assert read_caskade.tags[a4] == [a4_tag]
    # assert read_caskade.derived[a4][a4_permalink] == a4_derived

    # logit("all_matched")

    caskade.pause()
    # logit("pause")

    sp.add_check_point()

    write_caskade = caskade_cls(dir)
    assert write_caskade.check_points[0].type == CheckPointType.ON_CASK_HEADER

    assert caskade.check_points == write_caskade.check_points
    assert write_caskade.check_points[-1].type == CheckPointType.ON_CASKADE_PAUSE
    # logit("write_caskade")

    write_caskade.resume()
    # logit("resume")
    sp.add_check_point()
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

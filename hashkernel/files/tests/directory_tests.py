import asyncio
import sys
from pathlib import Path

import pytest
from hs_build_tools import LogTestOut
import re

from hashkernel import json_encode, to_json, json_decode
from hashkernel.bakery import Cake
from hashkernel.files.directory import DirContent, OnNewDirContent, \
    process_dir, FileExtra
from hashkernel.files.ignore_file import DEFAULT_IGNORE_POLICY
from hashkernel.files.tests import seed_file
from hashkernel.hashing import HashKey

log, out = LogTestOut.get(__name__)

scan_dir = Path(out.child_dir("scanning"))


def print_dc(dc: DirContent):
    print(dc.file.path)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dc_cb, file_extra_factory, ignore_symlinks",
    [
        (None, None, True),
        (None, None, False),
        (print_dc, HashKey.from_file, True),
        (print_dc, Cake.from_file, False),
    ],
)
async def test_ignore_policy(
    dc_cb: OnNewDirContent, file_extra_factory, ignore_symlinks: bool
):

    a_b_1_5 = seed_file(scan_dir / "a" / "b", 1, 5)
    x_f_b_1_5 = seed_file(scan_dir / "x" / "f" / "b", 1, 5)
    c_b_2_7 = seed_file(scan_dir / "c" / "b", 2, 7)
    c_b_1_5 = seed_file(scan_dir / "c" / "b", 1, 5)

    try:
        (scan_dir / "b").symlink_to(scan_dir / "a")
    except:
        pass

    rules = DEFAULT_IGNORE_POLICY.apply(scan_dir)
    rules.ignore_symlinks = ignore_symlinks
    entry = await process_dir(scan_dir, rules, dc_cb, file_extra_factory)
    assert entry.name() == "scanning"
    extras = entry.xtra.extras
    first_level = ["a", "c", "x"]
    if not ignore_symlinks:
        first_level.insert(1, "b")
        # same symlinked directories
        assert (
            extras[0].xtra.extras[0].xtra.extras[0].xtra
            == extras[1].xtra.extras[0].xtra.extras[0].xtra
        )
        assert json_encode(to_json(extras[0].xtra.extras[0])) == json_encode(
            to_json(extras[1].xtra.extras[0])
        )

    assert [e.name() for e in extras] == first_level

    json = json_encode(to_json(entry))

    fe = FileExtra.from_json(json_decode(json))
    assert [x.name() for x in fe.xtra] == [x.name() for x in
                                           entry.xtra.extras]
    for i in range(len(entry.xtra)):
        assert fe.xtra[i].name() == entry.xtra[i].name()

    if file_extra_factory == HashKey.from_file and ignore_symlinks:
        json = re.sub(r'"mod": "[^"]+",', '', json)
        print(json)
        assert json == \
               '{ "name": "scanning", "size": 22, "type": "TREE", "xtra": ' \
               '[{ "name": "a", "size": 5, "type": "TREE", "xtra": ' \
               '[{ "name": "b", "size": 5, "type": "TREE", "xtra": ' \
               '[{ "name": "1_5.dat", "size": 5, "type": "FILE", ' \
               '"xtra": "3j0h8nu9fxfn085rggvr4il4yq3x6ipoi1rv0oo2r8ixrlzqjv"}]}]}, ' \
               '{ "name": "c", "size": 12, "type": "TREE", "xtra": ' \
               '[{ "name": "b", "size": 12, "type": "TREE", "xtra": ' \
               '[{ "name": "1_5.dat", "size": 5, "type": "FILE", ' \
               '"xtra": "3j0h8nu9fxfn085rggvr4il4yq3x6ipoi1rv0oo2r8ixrlzqjv"}, ' \
               '{ "name": "2_7.dat", "size": 7, "type": "FILE", ' \
               '"xtra": "pkmdkuteqp50nvdl3o23v6egmbjnyb5mx65qicbtiulhipuzw"}]}]}, ' \
               '{ "name": "x", "size": 5, "type": "TREE", "xtra": ' \
               '[{ "name": "f", "size": 5, "type": "TREE", "xtra": ' \
               '[{ "name": "b", "size": 5, "type": "TREE", "xtra": ' \
               '[{ "name": "1_5.dat", "size": 5, "type": "FILE", ' \
               '"xtra": "3j0h8nu9fxfn085rggvr4il4yq3x6ipoi1rv0oo2r8ixrlzqjv"}]}]}]}]}'


async def main():

    path = Path(sys.argv[1]).absolute()
    print(
        json_encode(
            to_json(
                await process_dir(
                    path,
                    DEFAULT_IGNORE_POLICY.apply(path),
                    content_cb=print_dc,
                    file_extra_factory=Cake.from_file,
                )
            )
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

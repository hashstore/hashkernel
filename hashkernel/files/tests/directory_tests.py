import asyncio
import sys
from pathlib import Path

import pytest
from hs_build_tools import LogTestOut

from hashkernel import json_encode, to_json
from hashkernel.bakery import Cake
from hashkernel.files.directory import DirContent, OnNewDirContent, process_dir
from hashkernel.files.ignore_file import DEFAULT_IGNORE_POLICY
from hashkernel.files.tests import seed_file

log, out = LogTestOut.get(__name__)

scan_dir = Path(out.child_dir("scanning"))


def print_dc(dc: DirContent):
    print(dc.path)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dc_cb, file_extra_factory, ignore_symlinks",
    [
        (None, None, True),
        (None, None, False),
        (print_dc, Cake.from_file, True),
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
    assert entry.name == "scanning"
    entries = entry.xtra.entries
    first_level = ["a", "c", "x"]
    if not ignore_symlinks:
        first_level.insert(1, "b")
        # same symlinked directories
        assert (
            entries[0].xtra.entries[0].xtra.entries[0].xtra
            == entries[1].xtra.entries[0].xtra.entries[0].xtra
        )
        assert json_encode(to_json(entries[0].xtra.entries[0])) == json_encode(
            to_json(entries[1].xtra.entries[0])
        )
    assert [e.name for e in entries] == first_level


async def main():

    path = Path(sys.argv[1]).absolute()
    print(
        json_encode(
            to_json(
                await process_dir(
                    path,
                    DEFAULT_IGNORE_POLICY.apply(path),
                    callback=print_dc,
                    file_extra_factory=Cake.from_file,
                )
            )
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

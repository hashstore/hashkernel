from pathlib import Path

import pytest
from hs_build_tools import LogTestOut

from hashkernel.files.directory import process_dir
from hashkernel.files.ignore_file import (
    DEFAULT_IGNORE_POLICY,
    INCLUSIVE_POLICY,
    IgnoreFilePolicy,
    PathMatch,
)
from hashkernel.files.tests import dump_file, seed_file

log, out = LogTestOut.get(__name__)

ignorables_dir = Path(out.child_dir("ignorables"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "add_duplicates, policy",
    [
        (True, DEFAULT_IGNORE_POLICY),
        (False, DEFAULT_IGNORE_POLICY),
        (False, INCLUSIVE_POLICY),
    ],
)
async def test_ignore_policy(add_duplicates: bool, policy: IgnoreFilePolicy):
    svn_file = seed_file(ignorables_dir / ".svn", 1, 5)
    ab1_file = seed_file(ignorables_dir / "a" / "b", 1, 5)
    ab2_file = seed_file(ignorables_dir / "a" / "b", 2, 7)
    xb2_file = seed_file(ignorables_dir / "x" / "b", 3, 10)
    ignorefile = dump_file(ignorables_dir / ".ignore", f"{ab2_file.name}\nxyz\n")
    rules = policy.apply(ignorables_dir)
    rules.update_ignore_files(PathMatch(ignorables_dir, "x"))
    if add_duplicates:
        rules.update_ignore_files(PathMatch(ignorables_dir, "x"))
        rules.update_ignore_files(PathMatch(ignorables_dir, ab2_file.name))
        rules.update_spec_to_parse(ignorefile.name)
        rules.update_ignore_files("x")

    entry = await process_dir(ignorables_dir, rules)
    assert entry.name == "ignorables"
    entries = entry.xtra.entries

    expected_on_1st_level = [".ignore", "a"]
    expected_on_3st_level = ["1_5.dat"]
    if policy == INCLUSIVE_POLICY:
        expected_on_1st_level.insert(1, ".svn")
        expected_on_3st_level.append("2_7.dat")

    assert [e.name for e in entries] == expected_on_1st_level
    a_entries = entries[-1].xtra.entries
    assert [e.name for e in a_entries] == ["b"]
    b_entries = a_entries[0].xtra.entries
    assert [e.name for e in b_entries] == expected_on_3st_level

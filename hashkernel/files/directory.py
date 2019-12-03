import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, NamedTuple, Optional, Tuple

from hashkernel import CodeEnum
from hashkernel.files import PathFilter, any_path
from hashkernel.files.ignore_file import DEFAULT_IGNORE_POLICY, IgnoreRuleSet


class EntryType(CodeEnum):
    DIR = (1,)
    FILE = (2,)


class DirEntry(NamedTuple):
    name: str
    size: int
    mod: datetime
    type: EntryType
    extra: Optional[Any]

    def __str__(self):
        return f"{self.name},{self.type.name},{self.size},{self.mod.isoformat()}\n"


class File(NamedTuple):
    path: Path
    size: int
    mod: datetime

    @staticmethod
    def from_path(path: Path) -> "File":
        stat = path.stat()
        dt = datetime.utcfromtimestamp(stat.st_mtime)
        return File(path, stat.st_size, dt)

    def entry(self):
        return DirEntry(self.path.name, self.size, self.mod, EntryType.FILE, None)


def read_dir(path: Path, ignore_rules: IgnoreRuleSet) -> Tuple[List[File], List[Path]]:
    """
    Returns:
        files - files in directory
        dir_pathes - `Path` object pointing to immediate child direcories
    """
    files = []
    dir_pathes = []
    listdir = list(path.iterdir())
    ignore_rules.parse_specs(listdir)
    for child in filter(ignore_rules.path_filter, listdir):
        if child.is_dir():
            dir_pathes.append(child)
        elif child.is_file:
            files.append(File.from_path(child))
    return files, dir_pathes


OnNewDirContent = Callable[[Any], None]


class DirContent(NamedTuple):
    path: Path
    entries: List[DirEntry]

    def size(self):
        return sum(e.size for e in self.entries)

    def mod(self):
        if len(self.entries):
            return max(e.mod for e in self.entries)
        else:
            return datetime.utcfromtimestamp(0)

    def entry(self) -> DirEntry:
        return DirEntry(self.path.name, self.size(), self.mod(), EntryType.DIR, self)


async def process_dir(
    path: Path, ignore_rules: IgnoreRuleSet, callback: OnNewDirContent = None
) -> DirEntry:
    assert path.is_dir()
    files, dir_paths = await asyncio.get_event_loop().run_in_executor(
        None, read_dir, path, ignore_rules
    )
    entries = [f.entry() for f in files]
    entries.extend([await process_dir(p, ignore_rules, callback) for p in dir_paths])
    entries.sort(key=lambda e: e.name)
    content = DirContent(path, entries)
    if callback is not None:
        callback(content)
    return content.entry()


async def main():
    def print_dc(dc: DirContent):
        print(dc.path)
        print("".join(map(str, dc.entries)))
        print()

    path = Path(sys.argv[1]).absolute()
    print(await process_dir(path, DEFAULT_IGNORE_POLICY.apply(path), callback=print_dc))


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

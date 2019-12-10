import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, NamedTuple, Optional, Tuple, Union

from hashkernel import CodeEnum, to_json
from hashkernel.files.ignore_file import IgnoreRuleSet


class EntryType(CodeEnum):
    DIR = (1,)
    FILE = (2,)


class DirEntry(NamedTuple):
    name: str
    size: int
    mod: datetime
    type: EntryType
    xtra: Optional[Any]


    def __to_json__(self):
        return { "name":self.name,
                 "type":self.type.name,
                 "size":self.size,
                 "mod":self.mod.isoformat(),
                 "xtra":to_json(self.xtra),
        }


class File(NamedTuple):
    path: Path
    size: int
    mod: datetime
    type: EntryType

    @staticmethod
    def from_path(path: Path, et: EntryType) -> "File":
        stat = path.stat()
        dt = datetime.utcfromtimestamp(stat.st_mtime)
        return File(path, stat.st_size, dt, et)

    def entry(self, xtra: Any = None):
        return DirEntry(self.path.name, self.size, self.mod, self.type, xtra)


def read_dir(path: Path, ignore_rules: IgnoreRuleSet) -> Tuple[List[File], List[File]]:
    """
    Returns:
        files - files in directory
        dirs - `Path` object pointing to immediate child direcories
    """
    files = []
    dirs = []
    listdir = list(path.iterdir())
    ignore_rules.parse_specs(listdir)
    for child in filter(ignore_rules.path_filter, listdir):
        symlink_to_be_ignored = child.is_symlink() and ignore_rules.ignore_symlinks
        if child.is_dir() and not symlink_to_be_ignored:
            dirs.append(File.from_path(child, EntryType.DIR))
        elif child.is_file and not symlink_to_be_ignored:
            files.append(File.from_path(child, EntryType.FILE))
    return files, dirs


OnNewDirContent = Callable[[Any], None]


class DirContent(NamedTuple):
    path: Path
    entries: List[DirEntry]
    dir_entry: Optional[DirEntry]

    def size(self):
        return sum(e.size for e in self.entries)

    def mod(self):
        dt = (
            max(e.mod for e in self.entries)
            if len(self.entries)
            else datetime.utcfromtimestamp(0)
        )
        if self.dir_entry is not None:
            dt = max(self.dir_entry.mod, dt)
        return dt

    def entry(self) -> DirEntry:
        return DirEntry(self.path.name, self.size(), self.mod(), EntryType.DIR, self)

    def __to_json__(self):
        return {
            "entries": [ to_json(e) for e in self.entries]
        }


async def run_io(*args):
    return await asyncio.get_event_loop().run_in_executor(None, *args)


async def process_dir(
    dir: Union[Path, File],
    ignore_rules: IgnoreRuleSet,
    callback: OnNewDirContent = None,
    file_extra_factory: Callable[[Path], Any] = None,
) -> DirEntry:
    if isinstance(dir, File):
        dir_entry = dir.entry()
        path = dir.path
    else:
        dir_entry = None
        path = dir
    assert path.is_dir()
    files, dirs = await run_io(read_dir, path, ignore_rules)
    dir_futures = [
        process_dir(p, ignore_rules, callback, file_extra_factory) for p in dirs
    ]
    if file_extra_factory is not None:
        child_entries = [f.entry(await run_io(file_extra_factory, f.path)) for f in files]
    else:
        child_entries = [f.entry() for f in files]
    child_entries += [await f for f in dir_futures]
    child_entries.sort(key=lambda e: e.name)

    content = DirContent(path, child_entries, dir_entry)
    if callback is not None:
        callback(content)
    return content.entry()



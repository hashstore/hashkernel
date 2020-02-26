import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, List, NamedTuple, Optional, Tuple, \
    Union, Dict, cast
from dateutil.parser import parse as dt_parse

from hashkernel import CodeEnum, to_json, identity
from hashkernel.files.ignore_file import IgnoreRuleSet


class FileType(CodeEnum):
    TREE = (0,)
    DIR = (1,)
    FILE = (2,)


class File(NamedTuple):
    path: Path
    size: int
    mod: datetime
    type: FileType

    @staticmethod
    def from_path(path: Path, ft: FileType = None) -> "File":
        stat = path.stat()
        if ft is None:
            ft = FileType.DIR if path.is_dir() else FileType.FILE
        dt = datetime.utcfromtimestamp(stat.st_mtime)
        return File(path, stat.st_size, dt, ft)


class FileExtra(NamedTuple):
    file: File
    xtra: Optional[Any]

    def name(self):
        return self.file.path.name

    def __to_json__(self):
        return {
            "name": self.name(),
            "type": self.file.type.name,
            "size": self.file.size,
            "mod": self.file.mod.isoformat(),
            "xtra": to_json(self.xtra),
        }

    @staticmethod
    def from_json( json:Dict[str,Any], parent:Optional[Path] = None, file_extra=identity )->"FileExtra":
        name = json["name"]
        path = Path(name) if parent is None else parent / name
        file = File(path, json["size"], dt_parse(json["mod"]), FileType[json["type"]])
        xtra = json["xtra"]
        if xtra is None:
            return FileExtra(file, None)
        if isinstance(xtra, str):
            return FileExtra(file, file_extra(xtra))
        return FileExtra(file, [FileExtra.from_json(e, parent=file.path, file_extra=file_extra) for e in xtra])


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
            dirs.append(File.from_path(child, FileType.DIR))
        elif child.is_file and not symlink_to_be_ignored:
            files.append(File.from_path(child, FileType.FILE))
    return files, dirs


class DirContent:
    file: File
    extras: List[FileExtra]

    def __init__(self, file:File, extras:List[FileExtra]):
        self.file = file
        self.extras = extras

    def size(self):
        return sum(e.file.size for e in self.extras)

    def mod(self):
        dt = (
            max(e.file.mod for e in self.extras)
            if len(self.extras)
            else datetime.utcfromtimestamp(0)
        )
        dt = max(self.file.mod, dt)
        return dt

    def __len__(self):
        return len(self.extras)

    def __getitem__(self, i:int)->FileExtra:
        return self.extras[i]

    def tree_extra(self) -> FileExtra:
        return FileExtra(File(self.file.path, self.size(), self.mod(), FileType.TREE), self)

    def __to_json__(self):
        return [to_json(e) for e in self.extras]


OnNewDirContent = Callable[[DirContent], None]


async def run_io(*args):
    return await asyncio.get_event_loop().run_in_executor(None, *args)


async def process_dir(
    dir: Union[Path, File],
    ignore_rules: IgnoreRuleSet,
    content_cb: OnNewDirContent = None,
    file_extra_factory: Callable[[Path], Any] = None,
) -> FileExtra:
    if isinstance(dir, Path):
        dir = cast(File, await run_io(File.from_path, dir))
    assert dir.type != FileType.FILE
    files, dirs = await run_io(read_dir, dir.path, ignore_rules)
    dir_futures = [
        process_dir(child_dir, ignore_rules, content_cb, file_extra_factory)
        for child_dir in dirs
    ]
    if file_extra_factory is not None:
        child_extras = [
            FileExtra(f, await run_io(file_extra_factory, f.path)) for f in files
        ]
    else:
        child_extras = [FileExtra(f, None) for f in files]
    child_extras += [await f for f in dir_futures]
    child_extras.sort(key=lambda e: e.name())

    content = DirContent(dir, child_extras)
    if content_cb is not None:
        content_cb(content)
    return content.tree_extra()

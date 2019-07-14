from pathlib import Path
from typing import NamedTuple, Optional, Sequence, Set, Tuple

from nanotime import nanotime

from hashkernel import CodeEnum
from hashkernel.bakery import Cake
from hashkernel.packer import (
    GREEDY_BYTES,
    INT_8,
    INT_32,
    NANOTIME,
    UTF8_GREEDY_STR,
    ProxyPacker,
    TuplePacker,
)
from hashkernel.smattr import Mold


"""
Somewhat inspired by BitCask
"""


class CheckPointType(CodeEnum):
    ON_SIZE = (
        0,
        """
    when chunk from last checkpoint exeeded maximum size
    """,
    )
    ON_TIME = (
        1,
        """
    when there was some activity sinse last last checkpoint and 
    maximum checkpoint time is exeeded.
    """,
    )
    ON_CLOSE = (
        2,
        """
    when cask is closed for manipulation
    """,
    )


class Record(NamedTuple):
    entry_type: int
    entry_size: int
    tstamp: nanotime
    src: Cake


Record_PACKER = ProxyPacker(
    Record,
    TuplePacker(INT_8, INT_32, NANOTIME, Cake.__packer__),
    lambda rec: (rec.type, rec.entry_size, rec.tstamp, rec.src),
)


class DataEntry(NamedTuple):
    data: bytes


class JournalEntry(NamedTuple):
    value: Cake


class VtreeEntry(NamedTuple):
    value: Cake
    path: str


class CheckpointEntry(NamedTuple):
    start: int
    end: int
    chunk_id: Cake
    type: CheckPointType

class CheckpointEntry(NamedTuple):
    start: int
    end: int
    chunk_id: Cake
    type: CheckPointType

def map_tuple_packers(types, component_packers):
    packers = []
    for cls in types:
        mold = Mold(cls)
        comp_classes = (a.typing.val_cref.cls for a in mold.attrs.values())
        packers.append(
            TuplePacker(*map(lambda cls: component_packers[cls], comp_classes), cls=cls)
        )
    return packers


ENTRY_TYPES = (DataEntry, JournalEntry, VtreeEntry, CheckpointEntry)

ENTRY_PACKERS = map_tuple_packers(
    ENTRY_TYPES,
    {
        str: UTF8_GREEDY_STR,
        Cake: Cake.__packer__,
        bytes: GREEDY_BYTES,
        int: INT_32,
        CheckPointType: ProxyPacker(CheckPointType, INT_8, int),
    },
)


class CaskType(CodeEnum):
    ACTIVE = (
        0,
        """
        cask that actively populated by kernel process
        """,
    )

    CASK = (
        1,
        """
        cask closed for modification but still indexed in `Caskade`
        """,
    )

    SHADOW = (
        2,
        """
        cask that being synchronized with active cask on other host
        """,
    )

    EXPIRED = (
        3,
        """
        expired cask excluded from `Caskade` and will be cleaned soon
        """,
    )

    def cask_path(self, dir: Path, guid: Cake) -> Path:
        return dir / f"{guid.digest36()}.{self.name.lower()}"

    @staticmethod
    def split_file_name(file_name: str) -> Tuple["CaskType", Cake]:
        """
        Split name into cask's type and id

        Returns:
             ct - type of Cask
             cask_id - guid
        """
        digest, ext = file_name.split(".")
        cask_id = Cake.from_digest36(digest, CakeHeaders.CASK)
        return CaskType[ext.upper()], cask_id


def find_cask_by_guid(
    dir: Path, guid: Cake, types: Sequence[CaskType] = CaskType
) -> Optional[Path]:
    for ct in types:
        path = ct.cask_path(dir, guid)
        if path.exists():
            return path
    return None


class Caskade:
    """

    """


class CaskFile:
    """
    cask type: in append mode, shadow
    Ideas:
        CaskJournal - backbone of caskade


    """

    def __init__(self, path: Path, guid: Optional[Cake] = None):
        self.write_allowed = False
        if guid is None:
            if path.is_dir():
                self.guid = Cake.new_guid(CakeHeaders.CASK)
                self.write_allowed = True
                self.path = CaskType.ACTIVE.cask_path(path, self.guid)
            else:
                digest, extension = path.name.split(".")
                self.guid = Cake.from_digest36(digest, CakeHeaders.CASK)
                self.path = path.dir
        else:
            self.path = find_cask_by_guid(path, guid)
            self.guid = guid

    def keys(self) -> Set[Cake]:
        ...

    def __getitem__(self, item):
        ...

    def write_bytes(self, content: bytes) -> Cake:
        assert self.write_allowed
        ...

    def write_journal(self, src: Cake, value: Cake):
        assert self.write_allowed
        ...

    def write_path(self, src: Cake, path: str, value: Cake):
        assert self.write_allowed
        ...

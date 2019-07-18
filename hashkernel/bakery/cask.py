from pathlib import Path
from typing import Dict, NamedTuple, Optional, Sequence, Set, Tuple, Union

from nanotime import nanotime

from hashkernel import CodeEnum
from hashkernel.bakery import BlockStream, Cake, CakeHeaders, Journal
from hashkernel.packer import (
    GREEDY_BYTES,
    INT_8,
    INT_32,
    NANOTIME,
    UTF8_GREEDY_STR,
    ProxyPacker,
    TuplePacker,
    build_code_enum_packer,
)
from hashkernel.smattr import Mold, build_named_tuple_packer


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
    ON_SEGMENT = (
        2,
        """ 
    last entry in cask file when segment is closed, must be to be preceded
    NEXT_SEGMENT entry. This cask All entries will go to next segment's cask
    """,
    )
    ON_CLOSE = (
        3,
        """
    when cask is closed for manipulation
    """,
    )


_COMPONENTS_PACKERS = {
    str: UTF8_GREEDY_STR,
    Cake: Cake.__packer__,
    bytes: GREEDY_BYTES,
    int: INT_32,
    CheckPointType: build_code_enum_packer(CheckPointType),
}


def build_entry_packer(cls: type) -> TuplePacker:
    return build_named_tuple_packer(cls, lambda cls: _COMPONENTS_PACKERS[cls])


class DataEntry(NamedTuple):
    data: bytes


class JournalEntry(NamedTuple):
    value: Cake


class SetPathInVtreeEntry(NamedTuple):
    value: Cake
    path: str


class DeletePathInVtreeEntry(NamedTuple):
    path: str


class CheckpointEntry(NamedTuple):
    start: int
    end: int
    section_id: Cake
    type: CheckPointType


class EntryType(CodeEnum):
    """
    >>> [ (e,e.size) for e in EntryType] #doctest: +NORMALIZE_WHITESPACE
    [(<EntryType.DATA: 0>, None), (<EntryType.JOURNAL: 1>, 33),
    (<EntryType.SET_PATH_IN_VTREE: 2>, None), (<EntryType.DELETE_PATH_IN_VTREE: 3>, None),
    (<EntryType.CHECK_POINT: 4>, 42), (<EntryType.NEXT_SEGMENT: 5>, 0),
    (<EntryType.PREVIOUS_SEGMENT: 6>, 0), (<EntryType.FIRST_SEGMENT: 7>, 0)]

    """

    DATA = (
        0,
        DataEntry,
        """
    Data identified by `src` hash""",
    )

    JOURNAL = (
        1,
        JournalEntry,
        """
        Entry in `src` journal set to current `value`
        """,
    )

    SET_PATH_IN_VTREE = (
        2,
        SetPathInVtreeEntry,
        """
        `src` identify vtree and entry contains new `value` for `path`
        """,
    )

    DELETE_PATH_IN_VTREE = (
        3,
        DeletePathInVtreeEntry,
        """
        `src` identify vtree and entry contains `path` to deleted
        """,
    )

    CHECK_POINT = (
        4,
        CheckpointEntry,
        """ 
        `start` and `end` position and `section_id` hash of section and 
        `type` points to reason why checkpoint happened.
        """,
    )

    NEXT_SEGMENT = (
        5,
        None,
        """
        `src` has address of next cask segment. This entry has to 
        precede ON_SEGMENT checkpoint.
        """,
    )

    PREVIOUS_SEGMENT = (
        6,
        None,
        """
        `src` has address of prev cask segment. This entry has to 
        follow FIRST_SEGMENT entry in each continuing segment cask.  
        """,
    )

    FIRST_SEGMENT = (
        7,
        None,
        """
        `src` has address of first cask segment. It has to be first 
        entry in any cask.
        """,
    )

    def __init__(self, code, entry_cls, doc):
        CodeEnum.__init__(self, code, doc)
        self.entry_cls = entry_cls
        self.entry_packer = None
        if self.entry_cls is not None:
            self.entry_packer = build_entry_packer(self.entry_cls)
            self.size = self.entry_packer.size
        else:
            self.size = 0


class Record(NamedTuple):
    entry_type: EntryType
    tstamp: nanotime
    src: Cake


Record_PACKER = ProxyPacker(
    Record,
    TuplePacker(INT_8, INT_32, NANOTIME, Cake.__packer__),
    lambda rec: (rec.type, rec.entry_size, rec.tstamp, rec.src),
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

    def cask_path(self, guid: Cake) -> Path:
        return f"{guid.digest36()}.{self.name.lower()}"

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


class CaskFile:
    """
    cask type: in append mode, shadow
    Ideas:
        CaskJournal - backbone of caskade


    """

    __enforce_private = object()

    caskade: "Caskade"
    path: Path
    guid: Cake
    type: bool

    def __init__(self, enforce_private, caskade, path, guid, type):
        assert enforce_private == self.__enforce_private, "private constructor"
        self.caskade = caskade
        self.path = path
        self.guid = guid
        self.type = type

    @classmethod
    def active_cask(cls, caskade: "Caskade"):
        return cls.by_guid(caskade, Cake.new_guid(CakeHeaders.CASK), CaskType.ACTIVE)

    @classmethod
    def by_guid(cls, caskade: "Caskade", guid: Cake, cask_type: CaskType):
        path = caskade.dir / cask_type.cask_path(guid)
        return cls(cls.__enforce_private, caskade, path, guid, cask_type)

    @classmethod
    def by_file(cls, caskade: "Caskade", fname: str):
        digest, extension = fname.name.split(".")
        guid = Cake.from_digest36(digest, CakeHeaders.CASK)
        cask_type = CaskType(extension.upper())
        return cls(cls.__enforce_private, caskade, caskade.dir / fname, guid, cask_type)

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


class DataLocation(NamedTuple):
    offset: int
    size: int
    file: "CaskFile"


class Caskade:
    """

    """

    dir: Path
    casks: Dict[Cake, CaskFile]
    data: Dict[Cake, DataLocation]
    active: CaskFile
    shadows: Dict[Cake, CaskFile]
    journals: Dict[Cake, Journal]

    def __getitem__(self, id: Cake) -> Union[bytes, BlockStream]:
        ...

    def write_bytes(self, content: bytes) -> Cake:
        ...

    def write_journal(self, src: Cake, value: Cake):
        ...

    def write_path(self, src: Cake, path: str, value: Optional[Cake]):
        ...

from pathlib import Path, PurePath
from typing import NamedTuple, Optional, Sequence, Set, Tuple, Union

from nanotime import nanotime

from hashkernel import CodeEnum
from hashkernel.bakery import BlockStream, Cake, CakeType, CakeTypes
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
from hashkernel.smattr import SmAttr, build_named_tuple_packer
from hashkernel.time import TTL, nanotime_now


"""
Somewhat inspired by BitCask
"""


class CheckPointType(CodeEnum):
    MANUAL = (
        0,
        """
    Manual checkpoint. Checkpoint ignored if no activity been recorded 
    since previous one. 
    """,
    )
    ON_SIZE = (
        1,
        """
    when secion from last checkpoint about ot maximum size set in config
    """,
    )
    ON_TIME = (
        2,
        """
    maximum checkpoint TTL is exeeded. ignored if no activity been recorded 
    since previous one. 
    """,
    )
    ON_NEXT_CASK = (
        3,
        """ 
    last entry in cask file when cask is closed, must be to be preceded
    `NextCaskEntry` entry. 
    """,
    )
    ON_CASKADE_CLOSE = (
        4,
        """
    last entry in cask file when caskade is closed, must be to be preceded
    `NextCaskEntry` entry with `NULL_CAKE` in `src. Whole caskade directory 
    will be close for modification after that. 
    """,
    )


_COMPONENTS_PACKERS = {
    str: UTF8_GREEDY_STR,
    Cake: Cake.__packer__,
    bytes: GREEDY_BYTES,
    int: INT_32,
    CakeType: ProxyPacker(CakeType, INT_8, int, CakeTypes.map),
    CheckPointType: build_code_enum_packer(CheckPointType),
}


def build_entry_packer(cls: type) -> TuplePacker:
    return build_named_tuple_packer(cls, lambda cls: _COMPONENTS_PACKERS[cls])


class DataEntry(NamedTuple):
    data: bytes


class CheckpointEntry(NamedTuple):
    start: int
    end: int
    type: CheckPointType


class PreviousCaskEntry(NamedTuple):
    caskade_id: Cake
    checkpoint_id: Cake


class EntryType(CodeEnum):
    """
    >>> [ (e,e.size) for e in EntryType] #doctest: +NORMALIZE_WHITESPACE
    [(<EntryType.DATA: 0>, None), (<EntryType.CHECK_POINT: 1>, 9),
    (<EntryType.NEXT_CASK: 2>, 0), (<EntryType.PREVIOUS_CASK: 3>, 66)]

    """

    DATA = (
        0,
        DataEntry,
        """
        Data identified by `src` hash
        """,
    )

    CHECK_POINT = (
        1,
        CheckpointEntry,
        """ 
        `start` and `end` - absolute positions in cask filen 
        `src` - hash of section of data from previous checkpoint 
        `type` - reason why checkpoint happened
        """,
    )

    NEXT_CASK = (
        2,
        None,
        """
        `src` has address of next cask segment. This entry has to 
        precede ON_NEXT_CASK checkpoint.
        """,
    )

    PREVIOUS_CASK = (
        3,
        PreviousCaskEntry,
        """
        first entry in any cask file excluding first, `src` has 
        cask_id of previous cask. `checkpoint_id` has checksum
        hash that should be copied from previous's cask last 
        checkpoint `src`.  
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

    def cask_path(self, dir: Path, guid: Cake) -> Path:
        return dir / f"{guid.digest36()}.{self.name.lower()}"

    @staticmethod
    def split_file_name(path: Path) -> Optional[Tuple["CaskType", Cake]]:
        """
        Split name into cask's type and id

        Returns:
             ct - type of Cask
             cask_id - guid
        """
        ext = path.suffix[1:]
        if ext == "" or len(path.stem) < 32 or len(path.stem) > 50:
            return None
        cask_id = Cake.from_digest36(path.stem, CakeTypes.CASK)
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
        return cls.by_guid(caskade, Cake.new_guid(CakeTypes.CASK), CaskType.ACTIVE)

    @classmethod
    def by_guid(cls, caskade: "Caskade", guid: Cake, cask_type: CaskType):
        path = caskade.dir / cask_type.cask_path(guid)
        return cls(cls.__enforce_private, caskade, path, guid, cask_type)

    @classmethod
    def by_file(cls, caskade: "Caskade", fname: str):
        digest, extension = fname.name.split(".")
        guid = Cake.from_digest36(digest, CakeTypes.CASK)
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
    file: "CaskFile"
    offset: int
    size: int


# class GuidRef:
#     guid: Cake
#     data: Union[None, Journal, VirtualTree, DataLocation]

CHUNK_SIZE: int = 2 ** 21  # 2Mb
MAX_CASK_SIZE: int = 2 ** 31  # 2Gb

SIZE_OF_CLOSE_CASK_SEQUENCE = (
    Record_PACKER.size * 2 + EntryType.CHECK_POINT.entry_packer.size
)

MAX_CASK_SIZE_ADJUSTED = MAX_CASK_SIZE - SIZE_OF_CLOSE_CASK_SEQUENCE


class CaskadeConfig(SmAttr):
    origin: Cake
    checkpoint_ttl: Optional[TTL] = None
    checkpoint_size: int = 128 * CHUNK_SIZE
    auto_chunk_cutoff: int = 3 * CHUNK_SIZE / 2

    def cask_strategy(
        self,
        writen_bytes_since_previous_checkpoint: int,
        first_activity_after_last_checkpoint: Optional[nanotime],
        current_size: int,
        size_to_be_written: int,
    ) -> Tuple[nanotime, bool, bool]:
        """
        Returns:
            now - current time
            new_checkpoint_now - is it time for checkpoint
            new_cask_now - is it time for new cask
        """
        now = nanotime_now()
        if current_size + size_to_be_written > MAX_CASK_SIZE_ADJUSTED:
            return now, False, True  # new cask
        if writen_bytes_since_previous_checkpoint > 0:
            if (
                self.checkpoint_ttl is not None
                and first_activity_after_last_checkpoint is not None
            ):
                expires = self.checkpoint_ttl.expires(
                    first_activity_after_last_checkpoint
                )
                if expires.nanoseconds() < now.nanoseconds():
                    return now, True, False
            if (
                writen_bytes_since_previous_checkpoint + size_to_be_written
                > self.checkpoint_size
            ):
                return now, True, False
        return now, False, False


class Caskade:
    """

    """

    dir: Path
    config: CaskadeConfig
    active: CaskFile
    # casks: Dict[Cake, CaskFile]
    # data: Dict[Cake, DataLocation]
    # guids: Dict[Cake, GuidRef]

    def __init__(self, dir: Union[Path, str]):
        self.dir = Path(dir).absolute()
        list(self.dir.iterdir())

    def _config_file(self) -> Path:
        return self.dir / ".hs_caskade"

    def __getitem__(self, id: Cake) -> Union[bytes, BlockStream]:
        ...

    def write_bytes(self, content: bytes) -> Cake:
        ...

    def write_journal(self, src: Cake, value: Cake):
        ...

    def write_path(self, src: Cake, path: str, value: Optional[Cake]):
        ...


# class ActiveHistoryReconEntry(NamedTuple):
#     content: Cake
#
#
# class GuidReconEntry(NamedTuple):
#     type_overide: CakeType
#     content: Cake
#
#
# class JournalEntry(NamedTuple):
#     value: Cake
#
#
# class SetPathInVtreeEntry(NamedTuple):
#     value: Cake
#     path: str
#
#
# class DeletePathInVtreeEntry(NamedTuple):
#     path: str

# JOURNAL = (
#     1,
#     JournalEntry,
#     """
#     Entry in `src` journal set to current `value`
#     """,
# )
#
# SET_PATH_IN_VTREE = (
#     2,
#     SetPathInVtreeEntry,
#     """
#     `src` identify vtree and entry contains new `value` for `path`
#     """,
# )
#
# DELETE_PATH_IN_VTREE = (
#     3,
#     DeletePathInVtreeEntry,
#     """
#     `src` identify vtree and entry contains `path` to deleted
#     """,
# )
#     ACTIVE_HISTORY = (
#         8,
#         ActiveHistoryReconEntry,
#         """
#         `src` has address of prev cask segment. This entry has to
#         follow FIRST_SEGMENT entry in each continuing segment cask.
#         """,
#     )
#     GUID_RECON = (
#         9,
#         GuidReconEntry,
#         """
#         `src` has address of prev cask segment. This entry has to
#         follow FIRST_SEGMENT entry in each continuing segment cask.
#         """,
#     )

from pathlib import Path
from typing import Any, Dict, NamedTuple, Optional, Sequence, Tuple, Union

from nanotime import nanotime

from hashkernel import CodeEnum, dump_jsonable, load_jsonable
from hashkernel.bakery import NULL_CAKE, BlockStream, Cake, CakeType, CakeTypes
from hashkernel.files import FileBytes
from hashkernel.hashing import Hasher
from hashkernel.packer import (
    GREEDY_BYTES,
    INT_8,
    INT_32,
    NANOTIME,
    SIZED_BYTES,
    UTF8_GREEDY_STR,
    Packer,
    ProxyPacker,
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

    ON_CASKADE_PAUSE = (
        5,
        """
    Caskade is paused.  
    """,
    )

    ON_CASKADE_RESUME = (
        6,
        """
    Caskade is resumed. `start` and `end` point to ON_CASKADE_PAUSE 
    checkpoint data, that has to preceed this checkpoint
    """,
    )


_COMPONENTS_PACKERS = {
    str: UTF8_GREEDY_STR,
    Cake: Cake.__packer__,
    bytes: GREEDY_BYTES,
    int: INT_32,
    nanotime: NANOTIME,
    CakeType: ProxyPacker(CakeType, INT_8, int, CakeTypes.map),
    CheckPointType: build_code_enum_packer(CheckPointType),
}


def build_entry_packer(cls: type) -> Packer:
    if cls == bytes:
        return SIZED_BYTES
    return build_named_tuple_packer(cls, lambda cls: _COMPONENTS_PACKERS[cls])


class DataEntry(NamedTuple):
    data: bytes


class CheckpointEntry(NamedTuple):
    start: int
    end: int
    type: CheckPointType


class CaskHeaderEntry(NamedTuple):
    caskade_id: Cake
    checkpoint_id: Cake


class EntryType(CodeEnum):
    """
    >>> [ (e,e.size) for e in EntryType] #doctest: +NORMALIZE_WHITESPACE
    [(<EntryType.DATA: 0>, None), (<EntryType.CHECK_POINT: 1>, 9),
    (<EntryType.NEXT_CASK: 2>, 0), (<EntryType.CASK_HEADER: 3>, 66)]

    """

    DATA = (
        0,
        bytes,
        """
        Data identified by `src` hash
        """,
    )

    CHECK_POINT = (
        1,
        CheckpointEntry,
        """ 
        `start` and `end` - absolute positions in cask file 
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

    CASK_HEADER = (
        3,
        CaskHeaderEntry,
        """
        first entry in any cask, `src` has cask_id of previous cask 
        or NULL_CAKE. `checkpoint_id` has checksum hash that should be 
        copied from previous's cask last checkpoint `src` or NULL_CAKE 
        and caskade_id points to caskade_id of series of cask.  
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


_COMPONENTS_PACKERS[EntryType] = build_code_enum_packer(EntryType)


class Record(NamedTuple):
    entry_type: EntryType
    tstamp: nanotime
    src: Cake


Record_PACKER = build_named_tuple_packer(Record, lambda k: _COMPONENTS_PACKERS[k])


def pack_entry(rec: Record, entry: Any):
    packer = rec.entry_type.entry_packer
    return Record_PACKER.pack(rec) + packer.pack(entry)


CHUNK_SIZE: int = 2 ** 21  # 2Mb
CHUNK_SIZE_2x = CHUNK_SIZE * 2
MAX_CASK_SIZE: int = 2 ** 31  # 2Gb

SIZE_OF_CLOSE_CASK_SEQUENCE = (
    Record_PACKER.size * 2 + EntryType.CHECK_POINT.entry_packer.size
)

MAX_CASK_SIZE_ADJUSTED = MAX_CASK_SIZE - SIZE_OF_CLOSE_CASK_SEQUENCE


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


def find_cask_by_guid(
    dir: Path, guid: Cake, types: Sequence[CaskType] = CaskType
) -> Optional[Path]:
    for ct in types:
        path = ct.cask_path(dir, guid)
        if path.exists():
            return path
    return None


class DataLocation(NamedTuple):
    cask_id: Cake
    offset: int
    size: int


class SegmentTracker:
    hasher: Hasher
    start_offset: int
    current_offset: int
    is_data: bool = False
    first_activity_after_last_checkpoint: Optional[nanotime] = None
    writen_bytes_since_previous_checkpoint: int = 0

    def __init__(self, current_offset):
        self.hasher = Hasher()
        self.start_offset = self.current_offset = current_offset

    def update(self, data):
        sz = len(data)
        self.hasher.update(data)
        self.current_offset += sz
        if self.is_data:
            self.first_activity_after_last_checkpoint = nanotime_now()
            self.writen_bytes_since_previous_checkpoint += sz
        self.is_data = True

    def will_it_spill(
        self, config: "CaskadeConfig", time: nanotime, size_to_be_written: int
    ) -> Optional[CheckPointType]:
        """
        Returns:
            new_checkpoint_now - is it time for checkpoint
            new_cask_now - is it time for new cask
        """
        if self.current_offset + size_to_be_written > config.max_cask_size:
            return CheckPointType.ON_NEXT_CASK  # new cask
        if self.writen_bytes_since_previous_checkpoint > 0:
            if (
                config.checkpoint_ttl is not None
                and self.first_activity_after_last_checkpoint is not None
            ):
                expires = config.checkpoint_ttl.expires(
                    self.first_activity_after_last_checkpoint
                )
                if expires.nanoseconds() < time.nanoseconds():
                    return CheckPointType.ON_TIME
            if (
                self.writen_bytes_since_previous_checkpoint + size_to_be_written
                > config.checkpoint_size
            ):
                return CheckPointType.ON_SIZE
        return None

    def checkpoint(self, cpt: CheckPointType) -> Tuple[Record, CheckpointEntry]:
        return (
            Record(
                EntryType.CHECK_POINT,
                nanotime_now(),
                src=(Cake(None, digest=self.hasher.digest(), type=CakeTypes.NO_CLASS)),
            ),
            CheckpointEntry(self.start_offset, self.current_offset, cpt),
        )

    def next_tracker(self):
        return SegmentTracker(self.current_offset)


class CaskFile:
    """
    cask type: in append mode, shadow
    Ideas:
        CaskJournal - backbone of caskade


    """

    caskade: "Caskade"
    path: Path
    guid: Cake
    type: CaskType
    tracker: SegmentTracker

    def __init__(self, caskade: "Caskade", guid: Cake, cask_type: CaskType):
        self.caskade = caskade
        self.guid = guid
        self.type = cask_type
        self.path = cask_type.cask_path(caskade.dir, guid)

    @classmethod
    def by_file(cls, caskade: "Caskade", fpath: Path) -> Optional["CaskFile"]:
        try:
            cask_type = CaskType(fpath.suffix[1:].upper())
            guid = Cake.from_digest36(fpath.stem, CakeTypes.CASK)
            return cls(caskade, guid, cask_type)
        except (KeyError, AttributeError) as e:
            return None

    def create_file(
        self,
        tstamp=None,
        prev_cask_id: Cake = NULL_CAKE,
        checkpoint_id: Cake = NULL_CAKE,
    ):
        self.tracker = SegmentTracker(0)
        if tstamp is None:
            tstamp = nanotime_now()
        self.append_buffer(
            pack_entry(
                Record(EntryType.CASK_HEADER, tstamp, prev_cask_id),
                CaskHeaderEntry(self.caskade.caskade_id, checkpoint_id),
            ),
            mode="xb",
        )

    def append_buffer(
        self, buffer: bytes, mode="ab", content_size=None
    ) -> Optional[DataLocation]:
        """
        Appends buffer to the file
        :return: data location if `content_size` is provided
        """
        with self.path.open(mode) as fp:
            fp.write(buffer)
        self.tracker.update(buffer)
        if content_size is not None:
            offset = self.tracker.current_offset - content_size
            return DataLocation(self.guid, offset, content_size)

    def read_file(self):
        fbytes = FileBytes(self.path)
        curr_pos = 0
        while curr_pos < len(fbytes):
            rec, offset = Record_PACKER.unpack(fbytes, curr_pos)
            curr_pos = offset
            if rec.entry_type == EntryType.DATA:
                data_size, offset = rec.entry_type.entry_packer.size_packer.unpack(
                    fbytes, curr_pos
                )
                self.caskade._add_data_location(
                    rec.src, DataLocation(self.guid, offset, data_size)
                )
                curr_pos = offset + data_size
            elif rec.entry_type.entry_packer is not None:
                _, offset = rec.entry_type.entry_packer.unpack(fbytes, curr_pos)
                curr_pos = offset

    def write_checkpoint(self, cpt: CheckPointType):
        rec, entry = self.tracker.checkpoint(cpt)
        self.tracker = self.tracker.next_tracker()
        self.append_buffer(pack_entry(rec, entry))
        return rec.src

    def _deactivate(self):
        assert self.type == CaskType.ACTIVE
        prev_name = self.type.cask_path(self.caskade.dir, self.guid)
        self.type = CaskType.CASK
        now_name = self.type.cask_path(self.caskade.dir, self.guid)
        prev_name.rename(now_name)
        self.path = now_name
        del self.tracker

    def write_bytes(self, content: bytes, cake: Cake) -> DataLocation:
        record = Record(EntryType.DATA, nanotime_now(), cake)
        buffer = pack_entry(record, content)
        entry_sz = len(buffer)
        cp_type = self.tracker.will_it_spill(
            self.caskade.config, record.tstamp, entry_sz
        )
        content_size = len(content)
        # print(f'{self.guid} {self.tracker.current_offset} {content_size} {cp_type}')
        if cp_type is None:
            return self.append_buffer(buffer, content_size=content_size)
        elif cp_type == CheckPointType.ON_NEXT_CASK:
            new_cask_id = Cake.new_guid(
                CakeTypes.CASK, uniform_digest=self.guid.uniform_digest()
            )
            new_file = CaskFile(self.caskade, new_cask_id, CaskType.ACTIVE)
            checkpoint_id = self._do_end_cask_sequence(
                cp_type, record.tstamp, new_cask_id, new_file
            )
            self.caskade.active.create_file(
                tstamp=record.tstamp,
                prev_cask_id=self.guid,
                checkpoint_id=checkpoint_id,
            )
            return self.caskade.active.append_buffer(buffer, content_size=content_size)
        else:
            self.write_checkpoint(cp_type)
            return self.append_buffer(buffer, content_size=content_size)

    def _do_end_cask_sequence(
        self,
        cp_type: CheckPointType,
        tstamp: nanotime = None,
        next_cask_id=NULL_CAKE,
        new_file=None,
    ) -> Cake:
        """

        :param cp_type:
        :param record:
        :param next_cask_id:
        :param new_file:
        :return:
        """
        if tstamp is None:
            tstamp = nanotime_now()
        assert cp_type in (CheckPointType.ON_NEXT_CASK, CheckPointType.ON_CASKADE_CLOSE)
        assert next_cask_id != NULL_CAKE or cp_type == CheckPointType.ON_CASKADE_CLOSE
        cask_rec = Record(EntryType.NEXT_CASK, tstamp, next_cask_id)
        self.append_buffer(Record_PACKER.pack(cask_rec))
        checkpoint_id = self.write_checkpoint(cp_type)
        self._deactivate()
        self.caskade._set_active(new_file)
        return checkpoint_id

    def fragment(self, start: int, size: int):
        with self.path.open("rb") as fp:
            fp.seek(start)
            buff = fp.read(size)
            assert size == len(buff)
            return buff

    # def write_journal(self, src: Cake, value: Cake):
    #     assert self.write_allowed
    #     ...
    #
    # def write_path(self, src: Cake, path: str, value: Cake):
    #     assert self.write_allowed
    #     ...


# class GuidRef:
#     guid: Cake
#     data: Union[None, Journal, VirtualTree, DataLocation]


class CaskadeConfig(SmAttr):
    origin: Cake
    checkpoint_ttl: Optional[TTL] = None
    checkpoint_size: int = 128 * CHUNK_SIZE
    auto_chunk_cutoff: int = CHUNK_SIZE_2x
    max_cask_size: int = MAX_CASK_SIZE_ADJUSTED

    def __validate__(self):
        assert CHUNK_SIZE <= self.auto_chunk_cutoff <= CHUNK_SIZE_2x
        assert CHUNK_SIZE_2x < self.checkpoint_size
        assert CHUNK_SIZE_2x < self.max_cask_size <= MAX_CASK_SIZE_ADJUSTED


class Caskade:
    """

    """

    dir: Path
    config: CaskadeConfig
    active: Optional[CaskFile]
    casks: Dict[Cake, CaskFile]
    data_locations: Dict[Cake, DataLocation]
    # guids: Dict[Cake, GuidRef]

    def __init__(self, dir: Union[Path, str], config: Optional[CaskadeConfig] = None):
        self.dir = Path(dir).absolute()
        self.casks = {}
        self.data_locations = {}
        if not self.dir.exists():
            self.dir.mkdir(mode=0o0700, parents=True)
            self.caskade_id = Cake.new_guid(CakeTypes.CASK)
            if config is None:
                self.config = CaskadeConfig(origin=self.caskade_id)
            else:
                config.origin = self.caskade_id
                self.config = config
            dump_jsonable(self._config_file(), self.config)
            self._set_active(CaskFile(self, self.caskade_id, CaskType.ACTIVE))
            self.active.create_file()
        else:
            assert self.dir.is_dir()
            self.config = load_jsonable(self._config_file(), CaskadeConfig)
            self.caskade_id = self.config.origin
            for fpath in self.dir.iterdir():
                file = CaskFile.by_file(self, fpath)
                if file is not None and self.is_file_belong(file):
                    self.casks[file.guid] = file
            for k in sorted(
                self.casks.keys(), key=lambda k: -k.guid_header().time.nanoseconds()
            ):
                self.casks[k].read_file()

    def _set_active(self, file: CaskFile):
        self.active = file
        self.casks[self.active.guid] = self.active

    def is_file_belong(self, file: CaskFile):
        return file.guid.uniform_digest() == self.caskade_id.uniform_digest()

    def __getitem__(self, id: Cake) -> Union[bytes, BlockStream]:
        dp = self.data_locations[id]
        file: CaskFile = self.casks[dp.cask_id]
        buffer = file.fragment(dp.offset, dp.size)
        if id.type == CakeTypes.BLOCKSTREAM:
            return BlockStream(buffer)
        return buffer

    def __contains__(self, id: Cake) -> bool:
        return id in self.data_locations

    def write_bytes(self, content: bytes, ct: CakeType = CakeTypes.NO_CLASS) -> Cake:
        cake = Cake.from_bytes(content, ct)
        if cake not in self:
            dp = self.active.write_bytes(content, cake)
            self._add_data_location(cake, dp, content)
        return cake

    def close(self):
        self.active._do_end_cask_sequence(CheckPointType.ON_CASKADE_CLOSE)

    def _config_file(self) -> Path:
        return self.dir / ".hs_caskade"

    def _add_data_location(
        self, cake: Cake, dp: DataLocation, written_data: Optional[bytes] = None
    ):
        """
        Add data location, and when new data being written update cache.

        TODO: caching of `written_data` if appropriate/available
        """
        self.data_locations[cake] = dp

    # def write_journal(self, src: Cake, value: Cake):
    #     ...
    #
    # def write_path(self, src: Cake, path: str, value: Optional[Cake]):
    #     ...


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

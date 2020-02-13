import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union, NamedTuple, \
    ClassVar

from collections import defaultdict
from nanotime import nanotime

from hashkernel import LogicRegistry
from hashkernel.bakery import NULL_CAKE, BlockStream, Cake, CakeType, CakeTypes
from hashkernel.caskade import (
    AccessError,
    BaseEntries,
    CaskadeConfig,
    CaskadeMetadata,
    CaskHeaderEntry,
    CaskType,
    CheckPointType,
    DataLocation,
    DataValidationError,
    EntryType,
    NotQuietError,
    Record,
    Record_PACKER,
    SegmentTracker,
    DataLinkHeader, PAYLOAD_SIZE_PACKER, CheckpointHeader)
from hashkernel.files import ensure_path
from hashkernel.files.buffer import FileBytes
from hashkernel.hashing import HashKey, NULL_HASH_KEY
from hashkernel.time import nanotime_now



class CheckPoint(NamedTuple):
    cask_id: Cake
    checkpoint_id: HashKey
    start: int
    end: int
    type: CheckPointType


class ReadOptions(NamedTuple):
    validate_data:bool
    validate_checkpoints:bool
    validate_signatures:bool

VALIDATE_NONE = ReadOptions(validate_data=False, validate_checkpoints=False,
                validate_signatures=False)
VALIDATE_ALL = ReadOptions(validate_data=True, validate_checkpoints=True,
                validate_signatures=True)


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
        self.path = cask_type.cask_path(caskade.meta.dir, guid)
        self.tracker = None

    @classmethod
    def by_file(cls, caskade: "Caskade", fpath: Path) -> Optional["CaskFile"]:
        try:
            cask_type = CaskType[fpath.suffix[1:].upper()]
            guid = Cake.from_hash_key(HashKey(fpath.stem), CakeTypes.CASK)
            return cls(caskade, guid, cask_type)
        except (KeyError, AttributeError) as e:
            return None

    def create_file(
        self,
        tstamp=None,
        prev_cask_id: Cake = NULL_CAKE,
        checkpoint_id: HashKey = NULL_HASH_KEY
    ):
        self.tracker = SegmentTracker(0)
        if tstamp is None:
            tstamp = nanotime_now()
        self.append_buffer(
            self.caskade.meta.pack_entry(
                Record(BaseEntries.CASK_HEADER.code, tstamp),
                CaskHeaderEntry(self.caskade.meta.caskade_id, checkpoint_id, prev_cask_id ),
                None
            ),
            mode="xb",
        )
        # add virtual checkpoint from cask header
        self.caskade.check_points.append(
            CheckPoint(self.guid, checkpoint_id, 0, 0, CheckPointType.ON_CASK_HEADER)
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
        return None

    def read_file(
        self,
        curr_pos=0,
        check_point_collector=None,
        read_opts: ReadOptions = VALIDATE_NONE,
    ):
        """

        """
        fbytes = FileBytes(self.path)
        cp_index = 0
        if read_opts.validate_checkpoints:
            self.tracker = SegmentTracker(curr_pos)
        while curr_pos < len(fbytes):
            eh = self.caskade.new_entry_helper(self, fbytes, curr_pos, read_opts)
            if eh.has_logic():
                check_point_to_add = eh.load_entry()
                if check_point_to_add is not None and check_point_collector is not None:
                    check_point_collector.insert(cp_index, check_point_to_add)
                    cp_index += 1
                if self.tracker is not None:
                    self.tracker.update(fbytes[eh.start_of_entry:eh.end_of_entry])
            curr_pos = eh.end_of_entry


    def write_checkpoint(self, cpt: CheckPointType)->HashKey:
        rec, header = self.tracker.checkpoint(cpt)
        self.tracker = self.tracker.next_tracker()
        self.append_buffer(self.caskade.meta.pack_entry(rec, header,
                                                        self.caskade.meta.sign))
        self.caskade.check_points.append(CheckPoint(self.guid,  *header))
        return header.checkpoint_id

    def _deactivate(self):
        assert self.type == CaskType.ACTIVE
        prev_name = self.type.cask_path(self.caskade.meta.dir, self.guid)
        self.type = CaskType.CASK
        now_name = self.type.cask_path(self.caskade.meta.dir, self.guid)
        prev_name.rename(now_name)
        self.path = now_name
        self.tracker = None

    def write_bytes(self, content: bytes, hkey: HashKey) -> DataLocation:
        return self.write_entry(
            BaseEntries.DATA, hkey, content, content_size=(len(content))
        )

    def write_entry(
        self,
        et: EntryType,
        header: Any,
        payload: Any,
        tstamp: nanotime = None,
        content_size=None,
    ) -> Optional[DataLocation]:
        if tstamp is None:
            tstamp = nanotime_now()
        rec = Record(et.code, tstamp)
        buffer = self.caskade.meta.pack_entry(rec, header, payload)
        entry_sz = len(buffer)
        cp_type = self.tracker.will_it_spill(self.caskade.meta.config, tstamp, entry_sz)
        if cp_type is None:
            return self.append_buffer(buffer, content_size=content_size)
        elif cp_type == CheckPointType.ON_NEXT_CASK:
            new_cask_id = Cake.new_guid(
                CakeTypes.CASK, uniform_digest=self.guid.uniform_digest()
            )
            new_file = CaskFile(self.caskade, new_cask_id, CaskType.ACTIVE)
            checkpoint_id = self._do_end_cask_sequence(
                cp_type, tstamp, new_cask_id, new_file
            )
            self.caskade.active.create_file(
                tstamp=tstamp, prev_cask_id=self.guid, checkpoint_id=checkpoint_id
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
    ) -> HashKey:
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
        buff = self.caskade.meta.pack_entry(Record(BaseEntries.NEXT_CASK.code, tstamp), next_cask_id, None)
        self.append_buffer(buff)
        checkpoint_id = self.write_checkpoint(cp_type)
        self._deactivate()
        self.caskade._set_active(new_file)
        return checkpoint_id

    def __len__(self):
        return self.path.stat().st_size

    def fragment(self, start: int, size: int):
        with self.path.open("rb") as fp:
            fp.seek(start)
            buff = fp.read(size)
            assert size == len(buff)
            return buff


class EntryHelper(object):
    registry:ClassVar[LogicRegistry] = LogicRegistry()

    def __init__(self, cask:CaskFile, fbytes:FileBytes, curr_pos:int, read_opts:ReadOptions):
        self.cask = cask
        self.fbytes = fbytes
        self.start_of_entry = curr_pos
        self.read_opts = read_opts
        self.rec, new_pos = Record_PACKER.unpack(fbytes, curr_pos)
        self.entry_code = self.rec.entry_code
        entry_type: EntryType = self.cask.caskade.meta.entry_types.find_by_code(
            self.entry_code)
        if entry_type.header_packer is None:
            self.header = None
        else:
            self.header, new_pos = entry_type.header_packer.unpack(fbytes, new_pos)
        self.end_of_entry = self.end_of_header = new_pos
        if entry_type.payload_packer is None:
            self.payload_dl = None
        else:
            payload_size, new_pos = PAYLOAD_SIZE_PACKER.unpack(fbytes, new_pos)
            self.payload_dl = DataLocation(cask.guid, new_pos, payload_size)
            self.end_of_entry = new_pos + payload_size

    def has_logic(self)->bool:
        return self.registry.has(self.rec.entry_code)

    def load_entry(self)->Optional[CheckPoint]:
        return self.registry.get(self.rec.entry_code)(self)

    @registry.add(BaseEntries.DATA)
    def load_DATA(self):
        hkey: HashKey = self.header
        self.cask.caskade._add_data_location(hkey, self.payload_dl)

        if self.read_opts.validate_data:
            if HashKey.from_bytes(
                    self.payload_dl.load(self.fbytes)) != hkey:
                raise DataValidationError(hkey)

    @registry.add(BaseEntries.CASK_HEADER)
    def load_CASH_HEADER(self)->CheckPoint:
        cask_head: CaskHeaderEntry = self.header
        # add virtual checkpoint from cask header
        return CheckPoint(
            self.cask.guid,
            cask_head.checkpoint_id,
            0,
            0,
            CheckPointType.ON_CASK_HEADER,
        )

    @registry.add(BaseEntries.LINK)
    def load_LINK(self):
        assert self.payload_dl is None
        data_link: DataLinkHeader = self.header
        self.cask.caskade.datalinks[data_link.from_id][
            data_link.purpose] = data_link.to_id

    @registry.add(BaseEntries.CHECK_POINT)
    def load_CHECK_POINT(self)->CheckPoint:
        cp_entry: CheckpointHeader = self.header
        if self.payload_dl.size and self.read_opts.validate_signatures:
            if not self.cask.caskade.meta.validate_signature(
                    self.fbytes[self.start_of_entry:self.end_of_header],
                    self.payload_dl.load(self.fbytes),
            ):
                raise ValueError("Cannot validate")
        if self.read_opts.validate_checkpoints and self.cask.tracker.writen_bytes_since_previous_checkpoint > 0:
            calculated = HashKey(self.cask.tracker.hasher)
            if calculated != cp_entry.checkpoint_id:
                raise DataValidationError(
                    f'{calculated} != {cp_entry.checkpoint_id}')
            self.cask.tracker = self.cask.tracker.next_tracker()
        return CheckPoint(self.cask.guid, *cp_entry)


class Caskade:
    """

    """

    meta: CaskadeMetadata
    active: Optional[CaskFile]
    casks: Dict[Cake, CaskFile]
    data_locations: Dict[HashKey, DataLocation]
    check_points: List[CheckPoint]
    datalinks: Dict[Cake, Dict[int, HashKey]]

    def __init__(
        self,
        path: Union[Path, str],
        entry_types: Type[EntryType],
        config: Optional[CaskadeConfig] = None,
    ):
        self.casks = {}
        self.data_locations = {}
        self.datalinks = defaultdict(dict)
        self.check_points = []
        self.meta = CaskadeMetadata(ensure_path(path).absolute(), entry_types, config)
        if self.meta.just_created:
            self._set_active(CaskFile(self, self.meta.caskade_id, CaskType.ACTIVE))
            self.active.create_file()
        else:
            for fpath in self.meta.dir.iterdir():
                file = CaskFile.by_file(self, fpath)
                if file is not None and self.is_file_belong(file):
                    self.casks[file.guid] = file
            for k in sorted(
                self.casks.keys(), key=lambda k: -k.guid_header().time.nanoseconds()
            ):
                self.casks[k].read_file(check_point_collector=self.check_points)



    def _set_active(self, file: CaskFile):
        self.active = file
        if file is not None:
            self.casks[self.active.guid] = self.active

    def new_entry_helper(self, *args) -> EntryHelper:
        return EntryHelper(*args)

    def is_file_belong(self, file: CaskFile):
        return file.guid.uniform_digest() == self.meta.caskade_id.uniform_digest()

    def checkpoint(self):
        self.assert_write()
        self.active.write_checkpoint(CheckPointType.MANUAL)

    def __getitem__(self, id: HashKey) -> bytes:
        return self.read_bytes(id)

    def read_bytes(self, id: HashKey) -> bytes:
        dp = self.data_locations[id]
        file: CaskFile = self.casks[dp.cask_id]
        return file.fragment(dp.offset, dp.size)

    def __contains__(self, id: HashKey) -> bool:
        return id in self.data_locations

    def assert_write(self):
        if self.active is None or self.active.tracker is None:
            raise AccessError("not writable")

    def write_bytes(self, content: bytes, force:bool=False) -> HashKey:
        self.assert_write()
        hkey = HashKey.from_bytes(content)
        if force or hkey not in self:
            dp = self.active.write_bytes(content, hkey)
            self._add_data_location(hkey, dp, content)
        return hkey

    def set_link(self, link: Cake, purpose: int, data: HashKey) -> bool:
        """
        Ensures link.

        Returns:
              `True` if writen, `False` if exists and already pointing
              to right data.
        """
        assert link.is_guid
        if link not in self.datalinks or purpose not in self.datalinks[link] or self.datalinks[link][purpose] != data:
            self.assert_write()
            self.active.write_entry(BaseEntries.LINK, DataLinkHeader(link, purpose, data), None)
            self.datalinks[link][purpose] = data
            return True
        return False

    def pause(self):
        self.assert_write()
        self.active.write_checkpoint(CheckPointType.ON_CASKADE_PAUSE)
        self.active.tracker = None
        self.active = None

    def resume(self):
        last_cp: CheckPoint = self.check_points[-1]
        if last_cp.type == CheckPointType.ON_CASKADE_PAUSE:
            active_candidate: CaskFile = self.casks[last_cp.cask_id]
            if last_cp.end + self.meta.size_of_checkpoint() == len(active_candidate):
                active_candidate.tracker = SegmentTracker(last_cp.end)
                active_candidate.tracker.update(
                    active_candidate.fragment(
                        last_cp.end, self.meta.size_of_checkpoint()
                    )
                )
                self.active = active_candidate
                self.active.write_checkpoint(CheckPointType.ON_CASKADE_RESUME)
            else:
                raise ValueError(
                    f"{CheckPointType.ON_CASKADE_RESUME} is not last record"
                )
        else:
            raise ValueError(
                f"{str(last_cp.type)} != {str(CheckPointType.ON_CASKADE_RESUME)}"
            )

    def recover(self, quiet_time=None):
        last_cp: CheckPoint = self.check_points[-1]
        if last_cp.type in {
            CheckPointType.ON_NEXT_CASK,
            CheckPointType.ON_CASKADE_CLOSE,
        }:
            raise AccessError(f"Cask already closed: {last_cp.cask_id}")
        active_candidate: CaskFile = self.casks[last_cp.cask_id]
        size = len(active_candidate)
        if quiet_time is not None:
            time.sleep(quiet_time)
            if size != len(active_candidate):
                raise NotQuietError()
        active_candidate.read_file(
            last_cp.end, read_opts=VALIDATE_ALL
        )
        self.active = active_candidate
        self.active.write_checkpoint(CheckPointType.ON_CASKADE_RECOVER)

    def close(self):
        self.assert_write()
        self.active._do_end_cask_sequence(CheckPointType.ON_CASKADE_CLOSE)

    def _add_data_location(
        self, cake: HashKey, dp: DataLocation, written_data: Optional[bytes] = None
    ):
        """
        Add data location, and when new data being written update cache.

        TODO: caching of `written_data` if appropriate/available
        """
        self.data_locations[cake] = dp

    def process_sub_entry(self, rec: Record, header: Any):
        return False


class BaseCaskade(Caskade):
    def __init__(self, dir: Union[Path, str], config: Optional[CaskadeConfig] = None):
        Caskade.__init__(self, dir, BaseEntries, config)

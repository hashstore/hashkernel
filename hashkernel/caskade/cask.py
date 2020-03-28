import time
from collections import defaultdict
from pathlib import Path
from typing import Any, ClassVar, Dict, List, NamedTuple, Optional, Type, Union

from nanotime import nanotime

from hashkernel import LogicRegistry, dump_jsonable, load_jsonable
from hashkernel.ake import Rake, RootSchema
from hashkernel.caskade import (
    PAYLOAD_SIZE_PACKER,
    AccessError,
    BaseJots,
    CaskadeConfig,
    CaskHeaderEntry,
    CaskType,
    CheckpointHeader,
    CheckPointType,
    DataLink,
    DataLocation,
    DataValidationError,
    JotType,
    JotTypeCatalog,
    NotQuietError,
    SegmentTracker,
    Stamp,
    Stamp_PACKER,
    CaskId)
from hashkernel.files import ensure_path
from hashkernel.files.buffer import FileBytes
from hashkernel.ake import NULL_CAKE, Cake
from hashkernel.time import nanotime_now


class CheckPoint(NamedTuple):
    cask_id: CaskId
    checkpoint_id: Cake
    start: int
    end: int
    type: CheckPointType


class ReadOptions(NamedTuple):
    validate_data: bool
    validate_checkpoints: bool
    validate_signatures: bool


VALIDATE_NONE = ReadOptions(
    validate_data=False, validate_checkpoints=False, validate_signatures=False
)
VALIDATE_ALL = ReadOptions(
    validate_data=True, validate_checkpoints=True, validate_signatures=True
)

class CaskFile:
    """
    cask type: in append mode, shadow
    Ideas:
        CaskJournal - backbone of caskade


    """

    caskade: "Caskade"
    cask_id: CaskId
    path: Path
    idx: int
    type: CaskType
    tracker: SegmentTracker
    catalog: Optional[JotTypeCatalog] = None

    def __init__(self, caskade: "Caskade", cask_id: CaskId, cask_type: CaskType):
        self.caskade = caskade
        self.cask_id = cask_id
        self.type = cask_type
        self.path = cask_id.path(caskade.dir, cask_type)
        self.tracker = None

    @classmethod
    def by_file(cls, caskade: "Caskade", fpath: Path) -> Optional["CaskFile"]:
        try:
            cask_type = CaskType[fpath.suffix[1:].upper()]
            return cls(caskade, CaskId.from_str(fpath.stem), cask_type)
        except (KeyError, AttributeError) as e:
            return None

    def create_file(
        self,
        tstamp=None,
        checkpoint_id: Cake = NULL_CAKE,
    ):
        self.tracker = SegmentTracker(0)
        self.catalog = JotTypeCatalog(self.caskade.jot_types)
        if tstamp is None:
            tstamp = nanotime_now()
        self.append_buffer(
            self.pack_entry(
                Stamp(BaseJots.CASK_HEADER.code, tstamp),
                CaskHeaderEntry(
                    self.caskade.caskade_id,
                    checkpoint_id,
                    self.catalog.key,
                ),
                self.catalog.types.catalog(),
            ),
            mode="xb",
        )
        # add virtual checkpoint from cask header
        self.caskade.check_points.append(
            CheckPoint(self.cask_id, checkpoint_id, 0, 0, CheckPointType.ON_CASK_HEADER)
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
            return DataLocation(self.cask_id, offset, content_size)
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
                    self.tracker.update(fbytes[eh.start_of_entry : eh.end_of_entry])
            curr_pos = eh.end_of_entry

    def write_checkpoint(self, cpt: CheckPointType) -> Cake:
        rec, header = self.tracker.checkpoint(cpt)
        self.tracker = self.tracker.next_tracker()
        cp_buff = self.pack_entry(rec, header, self.caskade.config.sign)
        self.append_buffer(cp_buff)
        self.caskade.check_points.append(CheckPoint(self.cask_id, *header))
        return header.checkpoint_id

    def _deactivate(self):
        assert self.type == CaskType.ACTIVE
        prev_name = self.cask_id.path(self.caskade.dir, self.type)
        self.type = CaskType.CASK
        now_name = self.cask_id.path(self.caskade.dir, self.type)
        prev_name.rename(now_name)
        self.path = now_name
        self.tracker = None

    def write_bytes(self, content: bytes, hkey: Cake) -> DataLocation:
        return self.write_entry(
            BaseJots.DATA, hkey, content, content_size=(len(content))
        )

    def write_entry(
        self,
        et: JotType,
        header: Any,
        payload: Any,
        tstamp: nanotime = None,
        content_size=None,
    ) -> Optional[DataLocation]:
        if tstamp is None:
            tstamp = nanotime_now()
        rec = Stamp(et.code, tstamp)
        buffer = self.pack_entry(rec, header, payload)
        entry_sz = len(buffer)
        cp_type = self.tracker.will_it_spill(self.caskade.config, tstamp, entry_sz)
        if cp_type is None:
            return self.append_buffer(buffer, content_size=content_size)
        elif cp_type == CheckPointType.ON_NEXT_CASK:
            new_cask_id = self.cask_id.next_id()
            new_file = CaskFile(self.caskade, new_cask_id, CaskType.ACTIVE)
            checkpoint_id = self._do_end_cask_sequence( cp_type, new_file )
            self.caskade.active.create_file(
                tstamp=tstamp, checkpoint_id=checkpoint_id
            )
            return self.caskade.active.append_buffer(buffer, content_size=content_size)
        else:
            self.write_checkpoint(cp_type)
            return self.append_buffer(buffer, content_size=content_size)

    def _do_end_cask_sequence(
        self,
        cp_type: CheckPointType,
        new_file=None,
    ) -> Cake:
        """

        :param cp_type:
        :param record:
        :param next_cask_id:
        :param new_file:
        :return:
        """
        assert cp_type in (CheckPointType.ON_NEXT_CASK, CheckPointType.ON_CASKADE_CLOSE)
        checkpoint_id = self.write_checkpoint(cp_type)
        self._deactivate()
        self.caskade._set_active(new_file)
        return checkpoint_id

    def pack_entry(self, rec: Stamp, header: Any, payload: Any) -> bytes:
        et: JotType = self.catalog.types.find_by_code(rec.entry_code)
        return et.pack_entry(rec, header, payload)

    def __len__(self):
        return self.path.stat().st_size

    def fragment(self, start: int, size: int):
        with self.path.open("rb") as fp:
            fp.seek(start)
            buff = fp.read(size)
            assert size == len(buff)
            return buff


class EntryHelper(object):
    registry: ClassVar[LogicRegistry] = LogicRegistry()

    def __init__(
        self, cask: CaskFile, fbytes: FileBytes, curr_pos: int, read_opts: ReadOptions
    ):
        self.cask = cask
        self.fbytes = fbytes
        self.start_of_entry = curr_pos
        self.read_opts = read_opts
        self.rec, new_pos = Stamp_PACKER.unpack(fbytes, curr_pos)
        entry_code = self.rec.entry_code
        self.entry_type: JotType = self.cask.caskade.jot_types.find_by_code(entry_code)
        if self.entry_type.header_packer is None:
            self.header = None
        else:
            self.header, new_pos = self.entry_type.header_packer.unpack(fbytes, new_pos)
        self.end_of_entry = self.end_of_header = new_pos
        if self.entry_type.payload_packer is None:
            self.payload_dl = None
        else:
            payload_size, new_pos = PAYLOAD_SIZE_PACKER.unpack(fbytes, new_pos)
            self.payload_dl = DataLocation(cask.cask_id, new_pos, payload_size)
            self.end_of_entry = new_pos + payload_size

    def has_logic(self) -> bool:
        return self.registry.has(self.rec.entry_code)

    def load_entry(self) -> Optional[CheckPoint]:
        return self.registry.get(self.rec.entry_code)(self)

    def payload(self) -> Any:
        return self.entry_type.payload_packer.unpack_whole_buffer(
            self.payload_dl.load(self.fbytes)
        )

    @registry.add(BaseJots.DATA)
    def load_DATA(self):
        hkey: Cake = self.header
        self.cask.caskade._add_data_location(hkey, self.payload_dl)

        if self.read_opts.validate_data:
            if Cake.from_bytes(self.payload_dl.load(self.fbytes)) != hkey:
                raise DataValidationError(hkey)

    @registry.add(BaseJots.CASK_HEADER)
    def load_CASH_HEADER(self) -> CheckPoint:
        cask_head: CaskHeaderEntry = self.header
        payload = self.payload()
        self.cask.catalog = JotTypeCatalog(
            self.cask.caskade.jot_types, payload, expand=False
        )
        assert cask_head.catalog_id == self.cask.catalog.key
        # add virtual checkpoint from cask header
        return CheckPoint(
            self.cask.cask_id, cask_head.checkpoint_id, 0, 0, CheckPointType.ON_CASK_HEADER
        )

    @registry.add(BaseJots.LINK)
    def load_LINK(self):
        assert self.payload_dl is None
        data_link: DataLink = self.header
        self.cask.caskade.datalinks[data_link.from_id][
            data_link.link_type
        ] = data_link.to_id

    @registry.add(BaseJots.CHECK_POINT)
    def load_CHECK_POINT(self) -> CheckPoint:
        cp_entry: CheckpointHeader = self.header
        if self.payload_dl.size and self.read_opts.validate_signatures:
            if not self.cask.caskade.config.validate_signature(
                self.fbytes[self.start_of_entry : self.end_of_header],
                self.payload_dl.load(self.fbytes),
            ):
                raise ValueError("Cannot validate")
        if (
            self.read_opts.validate_checkpoints
            and self.cask.tracker.writen_bytes_since_previous_checkpoint > 0
        ):
            calculated = Cake(self.cask.tracker.hasher)
            if calculated != cp_entry.checkpoint_id:
                raise DataValidationError(f"{calculated} != {cp_entry.checkpoint_id}")
            self.cask.tracker = self.cask.tracker.next_tracker()
        return CheckPoint(self.cask.cask_id, *cp_entry)


class Caskade:
    """

    """

    dir: Path
    caskade_id: Rake
    config: CaskadeConfig
    active: Optional[CaskFile]
    casks: Dict[CaskId, CaskFile]
    cask_ids: List[CaskId]
    data_locations: Dict[Cake, DataLocation]
    check_points: List[CheckPoint]
    datalinks: Dict[Rake, Dict[int, Cake]]
    jot_types: Type[JotType]

    def __init__(
        self,
        path: Union[Path, str],
        jot_types: Type[JotType],
        config: Optional[CaskadeConfig] = None,
    ):
        self.casks = {}
        self.jot_types = jot_types
        self.data_locations = {}
        self.datalinks = defaultdict(dict)
        self.check_points = []
        self.dir = ensure_path(path).absolute()
        self.config = config
        if not self.dir.exists():
            self.dir.mkdir(mode=0o0700, parents=True)
            self.caskade_id = Rake.build_new(RootSchema.CASKADE)
            if config is None:
                self.config = CaskadeConfig(origin=self.caskade_id)
            else:
                config.origin = self.caskade_id
                self.config = config
            self._etc_dir().mkdir(mode=0o0700, parents=True)
            if self.config.signer is not None:
                self.config.signer.init_dir(self._etc_dir())
            dump_jsonable(self._config_file(), self.config)
            self.cask_ids = []
            self._set_active(CaskFile(self, CaskId(self.caskade_id, 0), CaskType.ACTIVE))
            self.active.create_file()
        else:
            assert self.dir.is_dir()
            self.config = load_jsonable(self._config_file(), CaskadeConfig)
            if self.config.signer is not None:
                self.config.signer.load_from_dir(self._etc_dir())
            self.caskade_id = self.config.origin

            for fpath in self.dir.iterdir():
                file = CaskFile.by_file(self, fpath)
                if file is not None and self.is_file_belong(file):
                    self.casks[file.cask_id] = file
            self.cask_ids = sorted(self.casks.keys(), reverse=True)
            assert len(self.cask_ids)
            self.casks[self.cask_ids[0]].read_file(
                check_point_collector=self.check_points
            )
            for k in self.cask_ids[1:]:
                self.casks[k].read_file(check_point_collector=self.check_points)
        self.config.validate_config()

    def _config_file(self) -> Path:
        return self._etc_dir() / "config.json"

    def _etc_dir(self) -> Path:
        return self.dir / ".hs_etc"

    def _set_active(self, file: CaskFile):
        self.active = file
        if file is not None:
            self.cask_ids.insert(0, self.active.cask_id)
            self.casks[self.active.cask_id] = self.active

    def latest_file(self) -> CaskFile:
        return self.casks[self.cask_ids[0]]

    def new_entry_helper(self, *args) -> EntryHelper:
        return EntryHelper(*args)

    def is_file_belong(self, file: CaskFile):
        return file.cask_id.caskade_id == self.caskade_id

    def checkpoint(self):
        self.assert_write()
        self.active.write_checkpoint(CheckPointType.MANUAL)

    def __getitem__(self, id: Cake) -> bytes:
        return self.read_bytes(id)

    def read_bytes(self, id: Cake) -> bytes:
        dp = self.data_locations[id]
        file: CaskFile = self.casks[dp.cask_id]
        return file.fragment(dp.offset, dp.size)

    def __contains__(self, id: Cake) -> bool:
        return id in self.data_locations

    def assert_write(self):
        if self.active is None or self.active.tracker is None:
            raise AccessError("not writable")

    def write_bytes(self, content: bytes, force: bool = False) -> Cake:
        self.assert_write()
        hkey = Cake.from_bytes(content)
        if force or hkey not in self:
            dp = self.active.write_bytes(content, hkey)
            self._add_data_location(hkey, dp, content)
        return hkey

    def set_link(self, link: Rake, link_type: int, data: Cake) -> bool:
        """
        Ensures link.

        Returns:
              `True` if writen, `False` if exists and already pointing
              to right data.
        """
        if (
            link not in self.datalinks
            or link_type not in self.datalinks[link]
            or self.datalinks[link][link_type] != data
        ):
            self.assert_write()
            self.active.write_entry(
                BaseJots.LINK, DataLink(link, link_type, data), None
            )
            self.datalinks[link][link_type] = data
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
            if last_cp.end + size_of_check_point(self) == len(active_candidate):
                active_candidate.tracker = SegmentTracker(last_cp.end)
                active_candidate.tracker.update(
                    active_candidate.fragment(last_cp.end, size_of_check_point(self))
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
        active_candidate.read_file(last_cp.end, read_opts=VALIDATE_ALL)
        self.active = active_candidate
        self.active.write_checkpoint(CheckPointType.ON_CASKADE_RECOVER)

    def close(self):
        self.assert_write()
        self.active._do_end_cask_sequence(CheckPointType.ON_CASKADE_CLOSE)

    def _add_data_location(
        self, cake: Cake, dp: DataLocation, written_data: Optional[bytes] = None
    ):
        """
        Add data location, and when new data being written update cache.

        TODO: caching of `written_data` if appropriate/available
        """
        self.data_locations[cake] = dp


def size_of_entry(et: JotType, payload_size: int = 0) -> int:
    size = Stamp_PACKER.size + et.header_size
    if et.payload_packer is None:
        assert payload_size == 0
    else:
        size_of_size = len(PAYLOAD_SIZE_PACKER.pack(payload_size))
        size += size_of_size + payload_size
    return size


def size_of_check_point(cascade: Caskade):
    return size_of_entry(BaseJots.CHECK_POINT, cascade.config.signature_size())


class BaseCaskade(Caskade):
    def __init__(self, dir: Union[Path, str], config: Optional[CaskadeConfig] = None):
        Caskade.__init__(self, dir, BaseJots, config)

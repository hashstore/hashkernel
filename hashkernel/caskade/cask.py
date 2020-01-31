from pathlib import Path
from typing import Optional, Any, Dict, List, Union, Type

import time
from nanotime import nanotime

from hashkernel.bakery import Cake, CakeTypes, BlockStream, NULL_CAKE, \
    CakeType
from hashkernel.caskade import CaskType, SegmentTracker, BaseEntries, \
    Record, CaskHeaderEntry, CheckPoint, CheckPointType, DataLocation, \
    Record_PACKER, DataValidationError, LinkEntry, EntryType, \
    CaskadeMetadata, AccessError, CaskadeConfig, NotQuietError
from hashkernel.files.buffer import FileBytes
from hashkernel.hashing import HashKey
from hashkernel.time import nanotime_now


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
        checkpoint_id: Cake = NULL_CAKE,
    ):
        self.tracker = SegmentTracker(0)
        if tstamp is None:
            tstamp = nanotime_now()
        self.append_buffer(
            self.caskade.meta.pack_entry(
                Record(BaseEntries.CASK_HEADER.code, tstamp, prev_cask_id),
                CaskHeaderEntry(self.caskade.meta.caskade_id, checkpoint_id),
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
        validate_data=False,
        validate_signatures=False,
        check_point_collector=None,
        tracker=None,
    ):
        """

        """
        fbytes = FileBytes(self.path)
        cp_index = 0

        while curr_pos < len(fbytes):
            rec, new_pos = Record_PACKER.unpack(fbytes, curr_pos)
            entry_type = self.caskade.meta.entry_types.find_by_code(
                rec.entry_code)
            entry_packer = entry_type.entry_packer
            check_point_to_add = None
            if rec.entry_code == BaseEntries.DATA.code:

                data_size, offset = entry_packer.size_packer.unpack(fbytes, new_pos)
                self.caskade._add_data_location(
                    rec.src, DataLocation(self.guid, offset, data_size)
                )
                # skip over actual data
                new_pos = offset + data_size

                if validate_data:
                    reconstructed = Cake.from_bytes(
                        fbytes[offset:new_pos], type=rec.src.type
                    )
                    if reconstructed != rec.src:
                        raise DataValidationError(str(rec.src))

            elif entry_packer is not None:
                entry, new_pos = entry_packer.unpack(fbytes, new_pos)

                if rec.entry_code == BaseEntries.CASK_HEADER.code:
                    head_entry: CaskHeaderEntry = entry
                    # add virtual checkpoint from cask header
                    check_point_to_add = CheckPoint(
                        self.guid,
                        head_entry.checkpoint_id,
                        0,
                        0,
                        CheckPointType.ON_CASK_HEADER,
                    )
                elif rec.entry_code == BaseEntries.PERMALINK.code:
                    link_entry: LinkEntry = entry
                    self.caskade.permalinks[link_entry.dest] = rec.src
                elif rec.entry_code == BaseEntries.CHECK_POINT.code:
                    cp_entry: CheckPointType = entry
                    check_point_to_add = CheckPoint(self.guid, rec.src, *cp_entry)
                    signature_size = self.caskade.meta.config.signature_size()
                    if validate_signatures:
                        if not self.caskade.meta.config.validate(
                            fbytes[curr_pos:new_pos],
                            fbytes[new_pos : new_pos + signature_size],
                        ):
                            raise ValueError("Cannot validate")
                    new_pos += signature_size
                elif self.caskade.process_sub_entry(rec, entry):
                    pass
                else:
                    raise AssertionError(f"What entry_type is it {rec.entry_type}")

            if check_point_to_add is not None and check_point_collector is not None:
                check_point_collector.insert(cp_index, check_point_to_add)
                cp_index += 1

            if tracker is not None:
                tracker.update(fbytes[curr_pos:new_pos])
            curr_pos = new_pos

    def write_checkpoint(self, cpt: CheckPointType):
        rec, entry = self.tracker.checkpoint(cpt)
        self.tracker = self.tracker.next_tracker()
        cp_buffer = self.caskade.meta.pack_entry(rec, entry)
        if self.caskade.meta.config.signature_size():
            cp_buffer += self.caskade.meta.config.signer.sign(cp_buffer)
        self.append_buffer(cp_buffer)
        self.caskade.check_points.append(CheckPoint(self.guid, rec.src, *entry))
        return rec.src

    def _deactivate(self):
        assert self.type == CaskType.ACTIVE
        prev_name = self.type.cask_path(self.caskade.meta.dir, self.guid)
        self.type = CaskType.CASK
        now_name = self.type.cask_path(self.caskade.meta.dir, self.guid)
        prev_name.rename(now_name)
        self.path = now_name
        self.tracker = None

    def write_bytes(self, content: bytes, cake: Cake) -> DataLocation:
        return self.write_entry(
            BaseEntries.DATA, cake, content, content_size=(len(content))
        )

    def write_entry(
        self,
        et: EntryType,
        src: Cake,
        entry: Any,
        tstamp: nanotime = None,
        content_size=None,
    ) -> Optional[DataLocation]:
        if tstamp is None:
            tstamp = nanotime_now()
        rec = Record(et.code, tstamp, src)
        buffer = self.caskade.meta.pack_entry(rec, entry)
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
        cask_rec = Record(BaseEntries.NEXT_CASK.code, tstamp, next_cask_id)
        self.append_buffer(Record_PACKER.pack(cask_rec))
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

class Caskade:
    """

    """

    meta: CaskadeMetadata
    active: Optional[CaskFile]
    casks: Dict[Cake, CaskFile]
    data_locations: Dict[Cake, DataLocation]
    check_points: List[CheckPoint]
    permalinks: Dict[Cake, Cake]

    def __init__(self, dir: Union[Path, str], entry_types:Type[EntryType], config: Optional[CaskadeConfig] = None):
        self.casks = {}
        self.data_locations = {}
        self.permalinks = {}
        self.check_points = []
        self.meta = CaskadeMetadata(Path(dir).absolute(), entry_types, config)
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

    def is_file_belong(self, file: CaskFile):
        return file.guid.uniform_digest() == self.meta.caskade_id.uniform_digest()

    def checkpoint(self):
        self.assert_write()
        self.active.write_checkpoint(CheckPointType.MANUAL)

    def __getitem__(self, id: Cake) -> Union[bytes, BlockStream]:
        buffer = self.read_bytes(id)
        if id.type == CakeTypes.BLOCKSTREAM:
            return BlockStream(buffer)
        return buffer

    def read_bytes(self, id: Cake) -> bytes:
        dp = self.data_locations[id]
        file: CaskFile = self.casks[dp.cask_id]
        return file.fragment(dp.offset, dp.size)

    def __contains__(self, id: Cake) -> bool:
        return id in self.data_locations

    def assert_write(self):
        if self.active is None or self.active.tracker is None:
            raise AccessError("not writable")

    def write_bytes(self, content: bytes, ct: CakeType = CakeTypes.NO_CLASS) -> Cake:
        self.assert_write()
        cake = Cake.from_bytes(content, ct)
        if cake not in self:
            dp = self.active.write_bytes(content, cake)
            self._add_data_location(cake, dp, content)
        return cake

    def set_permalink(self, data: Cake, link: Cake) -> bool:
        """
        Ensures permalink.

        Returns:
              `True` if writen, `False` if exists and already pointing
              to right data.
        """
        assert not (data.is_guid)
        assert link.is_guid
        if link not in self.permalinks or self.permalinks[link] != data:
            self.assert_write()
            self.active.write_entry(BaseEntries.PERMALINK, data, LinkEntry(dest=link))
            self.permalinks[link] = data
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
        active_candidate.tracker = SegmentTracker(last_cp.end)
        active_candidate.read_file(
            last_cp.end, validate_data=True, tracker=active_candidate.tracker
        )
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

    def process_sub_entry(self, rec:Record, entry:Any):
        return False


class BaseCaskade(Caskade):
    def __init__(self, dir: Union[Path, str], config: Optional[CaskadeConfig] = None):
        Caskade.__init__(self, dir, BaseEntries, config)

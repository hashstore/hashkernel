import os
from pathlib import Path
from typing import Any, Iterable, List, NamedTuple, Optional, Tuple, Type, Union, cast

from nanotime import nanotime

from hashkernel import CodeEnum, MetaCodeEnumExtended
from hashkernel.ake import Cake, Rake, RootSchema
from hashkernel.files.buffer import FileBytes
from hashkernel.hashing import B36, Hasher, HasherSigner, Signer
from hashkernel.packer import (
    ADJSIZE_PACKER_4,
    BOOL_AS_BYTE,
    GREEDY_BYTES,
    INT_8,
    INT_32,
    NANOTIME,
    UTF8_STR,
    FixedSizePacker,
    GreedyListPacker,
    Packer,
    PackerLibrary,
    ProxyPacker,
    build_code_enum_packer,
    ensure_packer,
    named_tuple_packer,
)
from hashkernel.smattr import SmAttr, build_named_tuple_packer
from hashkernel.time import TTL, nanotime_now
from hashkernel.typings import is_callable


"""
Somewhat inspired by BitCask

Cascade is interface for all public interactions   
"""


class AccessError(Exception):
    """
    file closed to modification
    """

    pass


class NotQuietError(Exception):
    """
    Cask changed during quiet period
    """

    pass


class DataValidationError(Exception):
    """
    Hash does not match
    """

    pass


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

    ON_CASKADE_RECOVER = (
        7,
        """
    Checkpoint on sucessful Caskade recovery .  
    """,
    )

    ON_CASK_HEADER = (
        8,
        """
    Virtual checkpoint entry: not being written on disk, but stored in 
    `Caskade.check_points` to help identify which file currently being 
    writen in even if no physical checkpoints yet occured.
    """,
    )


def named_tuple_resolver(cls: type) -> Packer:
    return build_named_tuple_packer(cls, PACKERS.get_packer_by_type)


PACKERS = PackerLibrary().register_all(
    (Rake, lambda _: Rake.__packer__),
    (Cake, lambda _: Cake.__packer__),
    (nanotime, lambda _: NANOTIME),
    (CodeEnum, build_code_enum_packer),
    (bytes, lambda _: GREEDY_BYTES),
    (SmAttr, lambda t: ProxyPacker(t, GREEDY_BYTES)),
    (NamedTuple, named_tuple_resolver),
)


class SurrogateEnum(NamedTuple):
    name: str
    value: Any


@PACKERS.register(named_tuple_packer(INT_8, UTF8_STR, ADJSIZE_PACKER_4, BOOL_AS_BYTE))
class CatalogItem(NamedTuple):
    entry_code: int
    entry_name: str
    header_size: int
    has_payload: bool

    def enum_item(self) -> SurrogateEnum:
        return SurrogateEnum(
            self.entry_name,
            (
                self.entry_code,
                FixedSizePacker(self.header_size) if self.header_size else None,
                GREEDY_BYTES if self.has_payload else None,
                self.entry_name,
            ),
        )


@PACKERS.register(named_tuple_packer(Rake.__packer__, INT_8, Cake.__packer__))
class DataLink(NamedTuple):
    from_id: Rake
    link_type: int  # 0-255
    to_id: Cake


@PACKERS.register(named_tuple_packer(NANOTIME, INT_8, Cake.__packer__))
class DataLinkHistory(NamedTuple):
    tstamp: nanotime
    link_type: int  # 0-255
    to_id: Cake


@PACKERS.register(
    named_tuple_packer(
        Cake.__packer__, INT_32, INT_32, build_code_enum_packer(CheckPointType)
    )
)
class CheckpointHeader(NamedTuple):
    checkpoint_id: Cake
    start: int
    end: int
    type: CheckPointType


@PACKERS.resolve
class CaskHeaderEntry(NamedTuple):
    caskade_id: Rake
    checkpoint_id: Cake
    catalog_id: Cake
    # TODO stop_cask: CaskId


@PACKERS.register(named_tuple_packer(INT_8, NANOTIME))
class Stamp(NamedTuple):
    entry_code: int
    tstamp: nanotime


Stamp_PACKER = PACKERS.get_packer_by_type(Stamp)

Catalog_PACKER = GreedyListPacker(CatalogItem, packer_lib=PACKERS)

PAYLOAD_SIZE_PACKER = ADJSIZE_PACKER_4


class JotType(CodeEnum):
    def __init__(
        self,
        code,
        header: Union[type, Packer, None],
        payload: Union[type, Packer, None],
        doc,
    ):
        CodeEnum.__init__(self, code, doc)
        self.header_packer = ensure_packer(header, PACKERS)
        self.payload_packer = ensure_packer(payload, PACKERS)
        if self.header_packer is None:
            self.header_size = 0
        else:
            assert self.header_packer.fixed_size()
            self.header_size = self.header_packer.size

    def build_catalog_item(self):
        return CatalogItem(
            self.code, self.name, self.header_size, self.payload_packer is not None
        )

    @classmethod
    def catalog(cls) -> List[CatalogItem]:
        return sorted([et.build_catalog_item() for et in cls])

    @classmethod
    def force_in(
        cls, other_catalog: List[CatalogItem], expand: bool
    ) -> Tuple[Type["JotType"], bool]:
        """
        Impose
        :param other_catalog:
        :param expand:
        :return:
        """
        cat_dict = {item.entry_code: item for item in cls.catalog()}
        add: List[Any] = []
        mismatch = []
        has_surrogates = False
        for other in other_catalog:
            if other.entry_code not in cat_dict:
                add.append(other.enum_item())
                has_surrogates = True
            elif cat_dict[other.entry_code] != other:
                mismatch.append(other)
            elif not expand:
                add.append(cls.find_by_code(other.entry_code))
        assert not mismatch, mismatch
        if expand:
            if not add:
                return cls, False
            return JotType.combine(cls, add), has_surrogates
        else:
            return JotType.combine(add), has_surrogates

    @staticmethod
    def combine(*enums: Iterable[Any]):
        class CombinedJotType(JotType, metaclass=MetaCodeEnumExtended, enums=[*enums]):
            pass

        return CombinedJotType

    @classmethod
    def extends(cls, *enums: Type["JotType"]):
        def decorate(decorated_enum: Type["JotType"]):
            return cls.combine(cls, decorated_enum, *enums)

        return decorate

    def pack_entry(self, rec: Stamp, header: Any, payload: Any) -> bytes:
        header_buff = Stamp_PACKER.pack(rec)
        if self.header_packer is not None:
            header_buff += self.header_packer.pack(header)
        return self.pack_payload(header_buff, payload)

    def pack_payload(self, header_buff: bytes, payload: Any):
        if self.payload_packer is None:
            assert payload is None
            payload_buff = b""
        else:
            assert payload is not None
            if is_callable(payload):
                payload = payload(header_buff)
            data_buff = self.payload_packer.pack(payload)
            payload_buff = PAYLOAD_SIZE_PACKER.pack(len(data_buff)) + data_buff
        return header_buff + payload_buff


class JotTypeCatalog:
    types: Type[JotType]
    binary: bytes
    key: Cake
    has_surrogates: bool

    def __init__(
        self,
        jot_types: Type[JotType],
        other_catalog: Optional[Union[bytes, List[CatalogItem]]] = None,
        expand: bool = True,
    ):
        if other_catalog is None:
            self.types = jot_types
            self.has_surrogates = False
        else:
            if isinstance(other_catalog, bytes):
                other_catalog = cast(
                    List[CatalogItem], Catalog_PACKER.unpack_whole_buffer(other_catalog)
                )
            self.types, self.has_surrogates = jot_types.force_in(other_catalog, expand)
        self.binary = Catalog_PACKER.pack(self.types.catalog())
        self.key = Cake.from_bytes(self.binary)

    def __len__(self):
        return len(self.binary)


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


@PACKERS.register(named_tuple_packer(Rake.__packer__, ADJSIZE_PACKER_4))
class CaskId(NamedTuple):
    caskade_id: Rake
    idx: int

    def __bytes__(self):
        return CASK_ID_PACKER.pack(self)

    @classmethod
    def from_str(cls, name: str) -> "CaskId":
        buffer = B36.decode(name.lower())
        return cast(CaskId, CASK_ID_PACKER.unpack_whole_buffer(buffer))

    def path(self, dir: Path, ct: CaskType):
        return dir / f"{B36.encode(bytes(self))}.{ct.name.lower()}"

    def next_id(self, add=1):
        return CaskId(self.caskade_id, self.idx + add)


CASK_ID_PACKER = PACKERS.get_packer_by_type(CaskId)


class BaseJots(JotType):

    CASK_HEADER = (
        0,
        CaskHeaderEntry,
        Catalog_PACKER,
        """
        first entry in any cask, `prev_cask_id` has cask_id of previous cask 
        or NULL_CAKE. `checkpoint_id` has checksum hash that should be 
        copied from `src` last checkpoint of previous cask or NULL_CAKE 
        and caskade_id points to caskade_id of series of cask.  
        """,
    )

    DATA = (
        1,
        Cake.__packer__,
        GREEDY_BYTES,
        """
        Data identified by header Cake
        """,
    )

    LINK = (2, DataLink, None, "Link")

    CHECK_POINT = (
        3,
        CheckpointHeader,
        GREEDY_BYTES,
        """ 
        `start` and `end` - absolute positions in cask file, start points 
         begining of previous checkpoint or begining of cask if there is 
         no checkpioints yet. `checkpointid` - hash of section beween 
         start and end. `type` - reason why checkpoint happened. Payload 
         contains signature fo header.
        """,
    )


CHUNK_SIZE: int = 2 ** 21  # 2Mb
CHUNK_SIZE_2x = CHUNK_SIZE * 2
MAX_CASK_SIZE: int = 2 ** 31  # 2Gb

NULL_CASKADE = Rake.null(RootSchema.CASKADE)


class DataLocation(NamedTuple):
    cask_id: CaskId
    offset: int
    size: int

    def load(self, fbytes: FileBytes) -> bytes:
        return fbytes[self.offset : self.end_offset()]

    def end_offset(self):
        return self.offset + self.size


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
                t = self.first_activity_after_last_checkpoint.nanoseconds()
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

    def checkpoint(self, cpt: CheckPointType) -> Tuple[Stamp, CheckpointHeader]:
        return (
            Stamp(BaseJots.CHECK_POINT.code, nanotime_now()),
            CheckpointHeader(
                Cake(self.hasher), self.start_offset, self.current_offset, cpt
            ),
        )

    def next_tracker(self):
        return SegmentTracker(self.current_offset)


class CaskSigner(Signer):
    def init_dir(self, etc_dir: Path):
        raise AssertionError("need to be implemented")

    def load_from_dir(self, etc_dir: Path):
        raise AssertionError("need to be implemented")


class CaskHashSigner(CaskSigner, HasherSigner):
    def _key_file(self, etc_dir: Path) -> Path:
        return etc_dir / "key.bin"

    def init_dir(self, etc_dir: Path):
        key = os.urandom(16)
        self._key_file(etc_dir).write_bytes(key)
        self._key_file(etc_dir).chmod(0o0600)
        HasherSigner.init(self, key)

    def load_from_dir(self, etc_dir: Path):
        key = self._key_file(etc_dir).read_bytes()
        HasherSigner.init(self, key)


CaskSigner.register("HashSigner", CaskHashSigner)


class CaskadeConfig(SmAttr):
    """
    >>> cc = CaskadeConfig(origin=NULL_CASKADE)
    >>> str(cc) #doctest: +NORMALIZE_WHITESPACE
    '{"auto_chunk_cutoff": 4194304, "checkpoint_size": 268435456, "checkpoint_ttl": null, "max_cask_size": 2147483648,
    "origin": "0000000000000001", "signer": null}'
    >>> cc2 = CaskadeConfig(str(cc))
    >>> cc == cc2
    True
    """

    origin: Rake
    max_cask_size: int = MAX_CASK_SIZE
    checkpoint_ttl: Optional[TTL] = None
    checkpoint_size: int = 128 * CHUNK_SIZE
    auto_chunk_cutoff: int = CHUNK_SIZE_2x
    signer: Optional[CaskSigner] = None

    def signature_size(self):
        return 0 if self.signer is None else self.signer.signature_size()

    def sign(self, header_buffer):
        signature = b""
        if self.signer is not None:
            signature = self.signer.sign(header_buffer)
        return signature

    def validate_signature(self, header_buffer: bytes, signature: bytes):
        if self.signer is not None:
            return self.signer.validate(header_buffer, signature)
        return False

    def validate_config(self):
        assert CHUNK_SIZE <= self.auto_chunk_cutoff <= CHUNK_SIZE_2x
        assert CHUNK_SIZE_2x < self.checkpoint_size
        assert CHUNK_SIZE_2x < self.max_cask_size <= MAX_CASK_SIZE

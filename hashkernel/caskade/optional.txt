from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Union

from hashkernel.bakery import Cake
from hashkernel.caskade import BaseEntries, CaskadeConfig, EntryType, Record
from hashkernel.caskade.cask import Caskade
from hashkernel.mold import MoldConfig
from hashkernel.smattr import SmAttr


class DerivedEntry(NamedTuple):
    filter: Cake
    derived: Cake


class Tag(SmAttr):
    """
    >>> str(Tag(name="abc"))
    '{"name": "abc"}'
    """

    __mold_config__ = MoldConfig(omit_optional_null=True)
    name: str
    value: Optional[float]
    link: Optional[Cake]


class StoreSyncPoints(SmAttr):
    """
    map of all caskades mapped by particular store
    """

    caskades: Dict[Cake, Cake]


class CaskadeSyncPoints(SmAttr):
    """
    map of all stores that tracking particular caskade
    """

    stores: Dict[Cake, Cake]


@BaseEntries.extends()
class OptionalEntries(EntryType):
    DERIVED = (
        5,
        DerivedEntry,
        """
        `src` - points to data Cake
        `filter` - filter points to logic that used to derive `src`
        `data` points to derived data  
        """,
    )

    TAG = (
        6,
        Tag,
        """
        `src` - points to Cake
        """,
    )

    CASCADE_SYNC = (
        7,
        Tag,
        """
        `src` - points to Cake
        """,
    )


class OptionalCaskade(Caskade):
    tags: Dict[Cake, List[Tag]]
    derived: Dict[Cake, Dict[Cake, Cake]]  # src -> filter -> derived_data

    def __init__(self, path: Union[Path, str], config: Optional[CaskadeConfig] = None):
        self.derived = defaultdict(dict)
        self.tags = defaultdict(list)
        Caskade.__init__(self, path, OptionalEntries, config)

    def tag(self, src: Cake, tag: Tag):
        self.assert_write()
        self.active.write_entry(OptionalEntries.TAG, src, tag)
        self.tags[src].append(tag)

    def save_derived(self, src: Cake, filter: Cake, derived: Cake):
        self.assert_write()
        self.active.write_entry(
            OptionalEntries.DERIVED, src, DerivedEntry(filter, derived)
        )
        self.derived[src][filter] = derived

    def process_sub_entry(self, rec: Record, entry: Any):
        if rec.entry_code == OptionalEntries.TAG.code:
            tag: Tag = entry
            self.tags[rec.src].append(tag)
        elif rec.entry_code == OptionalEntries.DERIVED.code:
            derived_entry: DerivedEntry = entry
            self.derived[rec.src][derived_entry.filter] = derived_entry.derived
        else:
            return False
        return True

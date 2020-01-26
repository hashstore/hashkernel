from pathlib import Path
from typing import NamedTuple, Optional, Dict, List, Union

from collections import defaultdict

from hashkernel import CodeEnum
from hashkernel.bakery import Cake
from hashkernel.caskade import Caskade, CaskadeConfig, AbstractEntryType
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

class OptionalEntryType(AbstractEntryType):
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

class OptionalCascade(Caskade):
    tags: Dict[Cake, List[Tag]]
    derived: Dict[Cake, Dict[Cake, Cake]]  # src -> filter -> derived_data

    def __init__(self, dir: Union[Path, str], config: Optional[CaskadeConfig] = None):
        Caskade.__init__(self, dir, config)
        self.derived = defaultdict(dict)
        self.tags = defaultdict(list)

    def tag(self, src: Cake, tag: Tag):
        self.assert_write()
        self.active.write_entry(OptionalEntryType.TAG, src, tag)
        self.tags[src].append(tag)

    def save_derived(self, src: Cake, filter: Cake, derived: Cake):
        self.assert_write()
        self.active.write_entry(OptionalEntryType.DERIVED, src, DerivedEntry(filter, derived))
        self.derived[src][filter] = derived

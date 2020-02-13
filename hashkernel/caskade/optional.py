from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Union, \
    ClassVar

from hashkernel import LogicRegistry
from hashkernel.bakery import Cake
from hashkernel.caskade import BaseEntries, CaskadeConfig, EntryType, Record
from hashkernel.caskade.cask import Caskade, EntryHelper
from hashkernel.hashing import HashKey
from hashkernel.mold import MoldConfig
from hashkernel.smattr import SmAttr


class DerivedHeader(NamedTuple):
    src: HashKey
    filter: Cake
    derived: HashKey


class Tag(SmAttr):
    """
    >>> str(Tag(name="abc"))
    '{"name": "abc"}'
    """

    __mold_config__ = MoldConfig(omit_optional_null=True)
    name: str
    value: Optional[float]
    link: Optional[Cake]


@BaseEntries.extends()
class OptionalEntries(EntryType):
    DERIVED = (
        6,
        DerivedHeader,
        None,
        """
        `src` - points to data Cake
        `filter` - filter points to logic that used to derive `src`
        `derived` - points to derived data  
        """,
    )

    TAG = (
        7,
        Cake,
        Tag,
        """
        `header` - points to Cake
        `payload` - tag body
        """,
    )

class OptionalEntryHelper(EntryHelper):
    registry:ClassVar[LogicRegistry] = LogicRegistry().add_all(EntryHelper.registry)

    @registry.add(OptionalEntries.TAG)
    def load_TAG(self):
        k: Cake = self.header
        tag: Tag = self.payload()
        self.cask.caskade.tags[k].append(tag)

    @registry.add(OptionalEntries.DERIVED)
    def load_DERIVED(self):
        drvd: DerivedHeader = self.header
        self.cask.caskade.derived[drvd.src][drvd.filter] = drvd.derived

class OptionalCaskade(Caskade):
    tags: Dict[Cake, List[Tag]]
    derived: Dict[HashKey, Dict[Cake, HashKey]]  # src -> filter -> derived_data

    def __init__(self, path: Union[Path, str], config: Optional[CaskadeConfig] = None):
        self.derived = defaultdict(dict)
        self.tags = defaultdict(list)
        Caskade.__init__(self, path, OptionalEntries, config)

    def tag(self, src: Cake, tag: Tag):
        self.assert_write()
        self.active.write_entry(OptionalEntries.TAG, src, tag)
        self.tags[src].append(tag)

    def save_derived(self, src: HashKey, filter: Cake, derived: HashKey):
        self.assert_write()
        self.active.write_entry(
            OptionalEntries.DERIVED, DerivedHeader(src, filter, derived), None
        )
        self.derived[src][filter] = derived

    def new_entry_helper(self, *args):
        return OptionalEntryHelper(*args)

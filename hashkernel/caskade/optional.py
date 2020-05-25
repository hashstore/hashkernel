from collections import defaultdict
from pathlib import Path
from typing import ClassVar, Dict, List, NamedTuple, Optional, Union

from hashkernel import LogicRegistry
from hashkernel.ake import Cake, Rake
from hashkernel.caskade import BaseJots, CaskadeConfig, JotType, Stamp
from hashkernel.caskade.cask import Caskade, EntryHelper
from hashkernel.mold import MoldConfig
from hashkernel.smattr import SmAttr


class DerivedHeader(NamedTuple):
    src: Cake
    filter: Rake
    derived: Cake


class Tag(SmAttr):
    """
    >>> str(Tag(name="abc"))
    '{"name": "abc"}'
    """

    __mold_config__ = MoldConfig(omit_optional_null=True)
    name: str
    value: Optional[float]
    link: Optional[Rake]


@BaseJots.extends()
class OptionalJots(JotType):
    DERIVED = (
        6,
        DerivedHeader,
        None,
        """
        `src` - points to original data
        `filter` - filter points to logic that used to derive `src`
        `derived` - points to derived data  
        """,
    )

    TAG = (
        7,
        Rake,
        Tag,
        """
        `header` - points to Rake
        `payload` - tag body
        """,
    )


class OptionalEntryHelper(EntryHelper):
    registry: ClassVar[LogicRegistry] = LogicRegistry().add_all(EntryHelper.registry)

    @registry.add(OptionalJots.TAG)
    def load_TAG(self):
        k: Rake = self.header
        tag: Tag = self.payload()
        self.cask.caskade.tags[k].append(tag)

    @registry.add(OptionalJots.DERIVED)
    def load_DERIVED(self):
        drvd: DerivedHeader = self.header
        self.cask.caskade.derived[drvd.src][drvd.filter] = drvd.derived


class OptionalCaskade(Caskade):
    tags: Dict[Rake, List[Tag]]
    derived: Dict[Cake, Dict[Rake, Cake]]  # src -> filter -> derived_data

    def __init__(self, path: Union[Path, str], config: Optional[CaskadeConfig] = None):
        self.derived = defaultdict(dict)
        self.tags = defaultdict(list)
        Caskade.__init__(self, path, OptionalJots, config)

    def tag(self, src: Rake, tag: Tag):
        self.assert_write()
        self.active.write_entry(OptionalJots.TAG, src, tag)
        self.tags[src].append(tag)

    def save_derived(self, src: Cake, filter: Rake, derived: Cake):
        self.assert_write()
        self.active.write_entry(
            OptionalJots.DERIVED, DerivedHeader(src, filter, derived), None
        )
        self.derived[src][filter] = derived

    def new_entry_helper(self, *args):
        return OptionalEntryHelper(*args)

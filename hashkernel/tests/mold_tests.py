from datetime import datetime
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple

from hs_build_tools.pytest import assert_text

from hashkernel import Jsonable
from hashkernel.mold import Flator, FunctionMold, Mold
from hashkernel.smattr import SmAttr

log = getLogger(__name__)


class A:
    """ An example of SmAttr usage

    Attributes:
       i: integer
       s: string with
          default
       d: optional datetime

       attribute contributed
    """

    i: int
    s: str = "xyz"
    d: Optional[datetime]
    z: List[datetime]
    y: Dict[str, str]


def test_docstring():
    amold = Mold(A)
    assert (
        str(amold)
        == '["i:Required[int]", "s:Required[str]=\\"xyz\\"", "d:Optional[datetime:datetime]", "z:List[datetime:datetime]", "y:Dict[str,str]"]'
    )
    assert_text(
        A.__doc__,
        """
     An example of SmAttr usage

    Attributes:
        i: Required[int] integer
        s: Required[str] string with default. Default is: 'xyz'.
        d: Optional[datetime:datetime] optional datetime
        z: List[datetime:datetime]
        y: Dict[str,str]

       attribute contributed
    """,
    )


def pack_wolves(i: int, s: str = "xyz") -> Tuple[int, str]:
    """ Greeting protocol

    Args:
       s: string with
          default

    Returns:
        num_of_wolves: Pack size
        pack_name: Name of the pack
    """
    return i, s


class PackId(SmAttr):
    nw: int
    name: str


def pack_wolves2(i: int, s: str = "xyz") -> PackId:
    return PackId((i, s))


def test_extract_molds_from_function():
    fn_mold = FunctionMold(pack_wolves)
    assert (
        str(fn_mold.out_mold)
        == '["num_of_wolves:Required[int]", "pack_name:Required[str]"]'
    )
    assert_text(
        fn_mold.dst.doc(),
        """ Greeting protocol

    Args:
        s: Required[str] string with default. Default is: 'xyz'.
        i: Required[int]

    Returns:
        num_of_wolves: Required[int] Pack size
        pack_name: Required[str] Name of the pack
    
    """,
    )
    assert fn_mold({"i": 5}) == {"num_of_wolves": 5, "pack_name": "xyz"}
    assert fn_mold({"i": 7, "s": "A-pack"}) == {
        "num_of_wolves": 7,
        "pack_name": "A-pack",
    }

    fn_mold2 = FunctionMold(pack_wolves2)
    assert fn_mold2({"i": 5}) == {"nw": 5, "name": "xyz"}
    assert fn_mold2({"i": 7, "s": "A-pack"}) == {"nw": 7, "name": "A-pack"}


class JsonableMemoryFlator(Flator):
    def __init__(self):
        self.store = []

    def is_applied(self, cls: type):
        return issubclass(cls, Jsonable)

    def inflate(self, k: str, cls: type):
        return cls(self.store[int(k)])

    def deflate(self, data: Any):
        k = str(len(self))
        self.store.append(str(data))
        return k

    def __len__(self):
        return len(self.store)


def test_flator():
    jmf = JsonableMemoryFlator()

    class X(SmAttr):
        a: int
        x: str
        q: bool

    def fn(z: X, t: int) -> bool:
        return True

    fn_mold = FunctionMold(fn)
    orig = {"z": X(a=5, x="s", q=False), "t": 6}
    deflated = fn_mold.in_mold.deflate(orig, jmf)
    assert deflated == {"z": "0", "t": 6}
    assert len(jmf) == 1
    back = fn_mold.in_mold.inflate(deflated, jmf)
    assert orig == back
    result = fn_mold(orig)
    assert result == {"_": True}

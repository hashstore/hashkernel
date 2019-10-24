from datetime import datetime
from logging import getLogger
from typing import Dict, List, Optional, Tuple

from hs_build_tools.pytest import assert_text

from hashkernel.mold import Mold, extract_molds_from_function

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


def test_extract_molds_from_function():
    in_mold, out_mold, dst = extract_molds_from_function(pack_wolves)
    assert str(out_mold) == '["num_of_wolves:Required[int]", "pack_name:Required[str]"]'
    assert_text(
        dst.doc(),
        """ Greeting protocol

    Args:
        s: Required[str] string with default. Default is: 'xyz'.
        i: Required[int]

    Returns:
        num_of_wolves: Required[int] Pack size
        pack_name: Required[str] Name of the pack
    
    """,
    )

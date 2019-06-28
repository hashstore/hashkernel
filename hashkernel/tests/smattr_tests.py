from datetime import datetime
from logging import getLogger
from typing import Dict, List, Optional, Tuple

from hs_build_tools.pytest import assert_text, ok_

from hashkernel import GlobalRef, exception_message, to_json
from hashkernel.smattr import (
    JsonWrap,
    SmAttr,
    extract_molds_from_function,
    typing_factory,
)

log = getLogger(__name__)


def test_docs():
    import doctest
    import hashkernel.smattr as smattr

    r = doctest.testmod(smattr)
    ok_(r.attempted > 0, f"There is not doctests in module")
    assert r.failed == 0


class A(SmAttr):
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


def test_docstring():
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


class Abc(SmAttr):
    name: str
    val: int


def test_wrap():
    abc = Abc({"name": "n", "val": 555})
    s = str(abc)

    def do_check(w):
        assert str(w.unwrap()) == s
        assert (
            str(w)
            == '{"classRef": "hashkernel.tests.smattr_tests:Abc", "json": {"name": "n", "val": 555}}'
        )
        assert str(JsonWrap(to_json(w)).unwrap()) == s

    do_check(JsonWrap({"classRef": GlobalRef(Abc), "json": {"name": "n", "val": 555}}))
    do_check(JsonWrap.wrap(abc))
    try:
        JsonWrap.wrap(5)
    except AttributeError:
        assert "Not jsonable: 5" == exception_message()

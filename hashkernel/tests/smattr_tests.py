from datetime import datetime
from logging import getLogger
from typing import Dict, List, Optional

from hs_build_tools.pytest import assert_text

from hashkernel import GlobalRef, exception_message, to_json
from hashkernel.smattr import JsonWrap, SmAttr

log = getLogger(__name__)


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

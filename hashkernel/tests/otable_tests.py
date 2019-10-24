from datetime import datetime
from logging import getLogger
from typing import Dict, List, Optional

from hashkernel import GlobalRef
from hashkernel.mold import typing_factory
from hashkernel.otable import OTable
from hashkernel.smattr import SmAttr

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


def test_gref_with_molded_table():
    ATable = OTable[A]
    t = ATable()
    assert str(t) == '#{"columns": ["i", "s", "d", "z", "y"]}\n'
    tn = "hashkernel.otable:OTable"
    assert str(GlobalRef(OTable)) == tn
    aref = str(GlobalRef(OTable[A]))
    assert aref == f"{tn}[hashkernel.tests.otable_tests:A]"
    assert ATable is OTable[A]
    a_table = GlobalRef(aref).get_instance()
    assert ATable is a_table


def test_typing_with_template():
    s = f"List[{GlobalRef(OTable[A])}]"
    tt = typing_factory(s)
    assert s == str(typing_factory(str(tt)))
    assert tt.val_cref.cls is OTable[A]

from datetime import datetime
from typing import Optional, List, Dict, Tuple
from logging import getLogger
from hs_build_tools.pytest import eq_, ok_, assert_text
from hashkernel import GlobalRef, exception_message, to_json
from hashkernel.otable import OTable
from hashkernel.smattr import (SmAttr, JsonWrap, typing_factory,
                               extract_molds_from_function)

log = getLogger(__name__)


def test_docs():
    import doctest
    import hashkernel.otable as m
    r = doctest.testmod(m)
    ok_(r.attempted > 0, f'There is not doctests in module')
    eq_(r.failed,0)


class A(SmAttr):
    """ An example of SmAttr usage

    Attributes:
       i: integer
       s: string with
          default
       d: optional datetime

       attribute contributed
    """
    i:int
    s:str = 'xyz'
    d:Optional[datetime]
    z:List[datetime]
    y:Dict[str,str]



def test_gref_with_molded_table():
    ATable = OTable[A]
    t = ATable()
    eq_(str(t), '#{"columns": ["i", "s", "d", "z", "y"]}\n')
    tn = 'hashkernel.otable:OTable'
    eq_(str(GlobalRef(OTable)), tn)
    aref = str(GlobalRef(OTable[A]))
    eq_(aref, f'{tn}[hashkernel.tests.otable_tests:A]')
    ok_(ATable is OTable[A])
    a_table = GlobalRef(aref).get_instance()
    ok_(ATable is a_table)

def test_typing_with_template():
    s = f'List[{GlobalRef(OTable[A])}]'
    tt = typing_factory(s)
    eq_(s, str(typing_factory(str(tt))))
    ok_(tt.val_cref.cls is OTable[A])

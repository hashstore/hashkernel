from logging import getLogger

import hashkernel.logic as logic
import hashkernel.tests.logic_test_module as plugin
from hashkernel import to_json

log = getLogger(__name__)


# def test_docs():
#     import doctest
#     r = doctest.testmod(logic)
#     assert (r.attempted > 0, 'There is not doctests in module')
#     assert r.failed ==0

# class Dag(logic.Task):
#     v:int
#     b = logic.Task(plugin.fn2)
#     a = logic.Task(plugin.fn, n=b.x, i=v)


def test_json():
    hl = logic.HashLogic.from_module(plugin)
    json = str(hl)
    match = (
        '{"methods": ['
        '{"in_mold": ['
        '"n:Required[hashkernel.ake:Cake]", '
        '"i:Required[int]"], '
        '"out_mold": ['
        '"_:Required[hashkernel.ake:Cake]"], '
        '"ref": "hashkernel.tests.logic_test_module:fn"}, '
        '{"in_mold": [], '
        '"out_mold": ['
        '"name:Required[str]", '
        '"id:Required[int]", '
        '"x:Required[hashkernel.ake:Cake]"], '
        '"ref": "hashkernel.tests.logic_test_module:fn2"}, '
        '{"in_mold": ['
        '"n:Required[hashkernel.ake:Cake]", '
        '"i:Required[int]=5"], '
        '"out_mold": ['
        '"_:Required[hashkernel.ake:Cake]"], '
        '"ref": "hashkernel.tests.logic_test_module:fn3"}], '
        '"name": "hashkernel.tests.logic_test_module"}'
    )
    assert json == match
    hl2 = logic.HashLogic(to_json(hl))
    assert str(hl2) == match

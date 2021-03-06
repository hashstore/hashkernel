from logging import getLogger

from hashkernel import exception_message
from hashkernel.auto_wire import AutoWire, AutoWireRoot, wire_names

log = getLogger(__name__)


def test_wiring():
    class Dependencies(AutoWire):
        _dependencies = []

        def add(self, depend_on: AutoWire) -> "Dependencies":
            self._dependencies.append(depend_on)
            return self

    x = Dependencies()

    z = x.y.z
    assert z._root() == None
    assert wire_names(z._path()) == ["", "y", "z"]

    class Dag(metaclass=AutoWireRoot):
        x = 3
        input = Dependencies()
        task1 = Dependencies().add(input.a)
        task2 = Dependencies().add(task1.input.v)
        output = Dependencies().add(task2.output.x)

    assert wire_names(Dag.input.a._path()) == ["input", "a"]
    assert wire_names(Dag.task1.input.v._path()) == ["task1", "input", "v"]

    assert Dag.input.a._root() == Dag
    assert Dag.task1.input.v._root() == Dag
    assert list(Dag._children.keys()) == ["input", "task1", "task2", "output"]

    try:
        q = x._q
        assert False
    except AttributeError:
        assert exception_message() == "no privates: _q"

from typing import NamedTuple

from hashkernel.ake import NULL_CAKE, Cake
from hashkernel.logic import DagMeta, EdgeMold, Task


def fn(n: Cake, i: int) -> Cake:
    print(f"n:{n} i:{i}")
    return n


class Worker(NamedTuple):
    name: str
    id: int
    x: Cake


def fn2() -> Worker:
    e = Worker("Guido", 5, NULL_CAKE)
    print(e)
    return e


def fn3(n: Cake, i: int = 5) -> Cake:
    print(f"fn3 n:{n} i:{i}")
    return n


class Dag(metaclass=DagMeta):
    input = EdgeMold()
    task1 = Task(fn2)
    task2 = Task(fn, n=task1.output.x, i=input.z)

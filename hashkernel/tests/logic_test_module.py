from typing import NamedTuple

from hashkernel.bakery import HashKey
from hashkernel.hashing import NULL_HASH_KEY
from hashkernel.logic import DagMeta, EdgeMold, Task


def fn(n: HashKey, i: int) -> HashKey:
    print(f"n:{n} i:{i}")
    return n


class Worker(NamedTuple):
    name: str
    id: int
    x: HashKey


def fn2() -> Worker:
    e = Worker("Guido", 5, NULL_HASH_KEY)
    print(e)
    return e


def fn3(n: HashKey, i: int = 5) -> HashKey:
    print(f"fn3 n:{n} i:{i}")
    return n


class Dag(metaclass=DagMeta):
    input = EdgeMold()
    task1 = Task(fn2)
    task2 = Task(fn, n=task1.output.x, i=input.z)

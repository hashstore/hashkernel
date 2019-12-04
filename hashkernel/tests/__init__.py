from random import Random
from time import perf_counter

from hashkernel import Stringable


class BytesGen:
    def __init__(self, seed=None):
        self.random = Random()
        if seed is None:
            self.random.seed(perf_counter(), version=2)
        else:
            self.random.seed(seed, version=2)

    def randint_repeat(self, start, end, repeat):
        return (self.random.randint(start, end) for _ in range(repeat))

    def get_bytes(self, length):
        return bytes(self.randint_repeat(0, 255, int(length)))


def rand_bytes(seed, size):
    return BytesGen(seed).get_bytes(size)


class StringableExample(Stringable):
    def __init__(self, s):
        self.s = s

    def __str__(self):
        return self.s

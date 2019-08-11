from random import Random
from time import clock


class BytesGen:
    def __init__(self, seed=None):
        self.random = Random()
        if seed is None:
            self.random.seed(clock(), version=2)
        else:
            self.random.seed(seed, version=2)

    def randint_repeat(self, start, end, repeat):
        return (self.random.randint(start, end) for _ in range(repeat))

    def random_bytes(self, length):
        return bytes(self.randint_repeat(0, 255, int(length)))

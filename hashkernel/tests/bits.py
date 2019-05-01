from hashkernel import Stringable, Jsonable


class StringableExample(Stringable):
    def __init__(self, s):
        self.s = s

    def __str__(self):
        return self.s




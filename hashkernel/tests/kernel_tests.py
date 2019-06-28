import abc
import datetime
import sys
from logging import getLogger

from hs_build_tools.pytest import assert_text,  ok_

import hashkernel as kernel
import hashkernel.docs as docs
from hashkernel.tests import StringableExample

log = getLogger(__name__)


# def test_docs():
#     import doctest
#     import hashkernel.time as time
#     import hashkernel.hashing as hashing
#     import hashkernel.typings as typings
#     import hashkernel.log_box as log_box
#
#     for t in (kernel, time, hashing, typings,
#               docs, log_box):
#         r = doctest.testmod(t)
#         ok_(r.attempted > 0, f'There is no doctests in module {t}')
#         assert r.failed ==0


def test_reraise():
    class _Ex(Exception):
        def __init__(self, a, b):
            Exception.__init__(self, a + " " + b)

    for e_type in range(3):
        for i in range(2):
            try:
                try:
                    if e_type == 0:
                        raise ValueError("EOF")
                    elif e_type == 1:
                        raise _Ex("a", "EOF")
                    else:
                        eval("hello(")
                except:
                    if i == 0:
                        kernel.reraise_with_msg("bye")
                    else:
                        kernel.reraise_with_msg("bye", sys.exc_info()[1])
            except:
                e = sys.exc_info()[1]
                msg = kernel.exception_message(e)
                ok_("EOF" in msg)
                ok_("bye" in msg)


class StringableIterable(StringableExample):
    def __iter__(self):
        yield self.s


class JsonableExample(kernel.Jsonable):
    def __init__(self, s, i):
        self.s = s
        self.i = i

    def __to_json__(self):
        return {"s": self.s, "i": self.i}

    def __to_tuple__(self):
        return self.s, self.i

    def __to_dict__(self):
        return self.__to_json__()


def test_ables():
    x = StringableIterable("x")
    assert bytes(x) == b"x"

    z5 = JsonableExample("z", 5)
    assert bytes(z5) == b'{"i": 5, "s": "z"}'
    z3 = JsonableExample("z", 3)
    z5too = JsonableExample("z", 5)
    ok_(z5 == z5too)
    ok_(z5 != z3)
    ok_(not (z5 == z3))

    assert kernel.to_dict(z5) == kernel.to_json(z5)
    assert kernel.to_tuple(z5) == ("z", 5)

    assert kernel.to_tuple(x) == ("x",)
    try:
        kernel.to_dict(x)
        ok_(False)
    except NotImplementedError:
        ...


def test_json_encode_decode():
    class q:
        pass

    try:
        kernel.json_encode(q())
        ok_(False)
    except:
        ok_("is not JSON serializable" in kernel.exception_message())

    assert kernel.json_encode(datetime.datetime(2019, 4, 26, 19, 46, 50, 217946)) == \
        '"2019-04-26T19:46:50.217946"'
    assert kernel.json_encode(datetime.date(2019, 4, 26)) == '"2019-04-26"'
    assert kernel.json_encode(JsonableExample("z", 5)) == '{"i": 5, "s": "z"}'
    assert kernel.json_decode('{"i": 5, "s": "z"}') == {"i": 5, "s": "z"}
    try:
        kernel.json_decode('{"i": 5, "s": "z"')
        ok_(False)
    except ValueError:
        ok_('text=\'{"i": 5, "s": "z"\'' in kernel.exception_message())


def test_mix_in():
    class StrKeyAbcMixin(metaclass=abc.ABCMeta):
        @abc.abstractmethod
        def __str__(self):
            raise NotImplementedError("subclasses must override")

    kernel.mix_in(kernel.StrKeyMixin, StrKeyAbcMixin)

    class B1(StrKeyAbcMixin):
        def __init__(self, k):
            self.k = k

        def __str__(self):
            return self.k

    class B2:
        def __init__(self, k):
            self.k = k

        def __str__(self):
            return self.k

    assert kernel.mix_in(kernel.StrKeyMixin, B2) == ["_StrKeyMixin__cached_str", "__eq__", "__hash__", "__ne__"]

    class B3(kernel.StrKeyMixin):
        def __init__(self, k):
            self.k = k

        def __str__(self):
            return self.k

    class B4:
        def __init__(self, k):
            self.k = k

        def __str__(self):
            return self.k

    class B5:
        ...

    class B6:
        def __eq__(self, other):
            raise NotImplementedError()

    class B7:
        def __eq__(self, other):
            raise NotImplementedError()

    assert kernel.mix_in(B1, B6) == [
            "_StrKeyMixin__cached_str",
            "__eq__",
            "__hash__",
            "__init__",
            "__ne__",
            "__str__",
        ]
    assert kernel.mix_in(B1, B7, lambda s, _: s != "__eq__") == ["_StrKeyMixin__cached_str", "__hash__", "__init__", "__ne__", "__str__"]
    kernel.mix_in(B4, B5)
    kernel.mix_in(kernel.StrKeyMixin, B5)

    def retest(B, match=(False, True, True, False)):
        assert (B("a") != B("a")) == match[0]
        assert (B("a") != B("b")) == match[1]
        assert (B("a") == B("a")) == match[2]
        assert (B("a") == B("b")) == match[3]

    retest(B1)
    retest(B2)
    retest(B3)
    retest(B4, (True, True, False, False))
    retest(B5)
    retest(B6)

    ok_(B6("x") == B6("x"))
    try:
        B7("x") == B7("x")
        ok_(False)
    except NotImplementedError:
        ...

    ok_(isinstance(B6("x"), StrKeyAbcMixin))
    ok_(not (isinstance(B5("x"), StrKeyAbcMixin)))


class A:
    """ An example of SmAttr usage

    Attributes:
        possible atributes of class
        i: integer
        s: string with
            default
        d: optional datetime

       attribute contributed
    """

    pass


class A_ValueError:
    """ An example of SmAttr usage

    Attributes:
        possible atributes of class
        i: integer
        s: string with
            default
        d: optional datetime
       attribute contributed
    """

    pass


def hello(i: int, s: str = "xyz") -> int:
    """ Greeting protocol

    Args:
       s: string with
          default

    Returns:
        _: very important number
    """
    pass


def test_doc_str_template():
    dst = docs.DocStringTemplate(hello.__doc__, {"Args", "Returns"})
    assert dst.var_groups["Args"].keys() == {"s"}
    assert dst.var_groups["Returns"].keys() == {"_"}
    assert list(dst.var_groups["Returns"].format(4)) == ["    Returns:", "        _: very important number"]
    assert_text(dst.doc(), hello.__doc__)

    dst = docs.DocStringTemplate(A.__doc__, {"Attributes"})
    attributes_ = dst.var_groups["Attributes"]
    assert attributes_.keys(), {"i", "s", "d"}
    assert list(attributes_.format(4)) == [
            "    Attributes:",
            "        possible atributes of class",
            "        i: integer",
            "        s: string with default",
            "        d: optional datetime",
        ]
    assert str(attributes_["s"].content) == "string with default"
    assert_text(dst.doc(), A.__doc__)

    attributes_.init_parse()  # no harm to call it second time

    try:
        docs.DocStringTemplate(A_ValueError.__doc__, {"Attributes"})
        ok_(False)
    except ValueError as e:
        assert str(e) == "Missleading indent=7? var_indent=8 " "line='attribute contributed' "

    dstNone = docs.DocStringTemplate(None, {})
    assert dstNone.doc() == ""


def test_CodeEnum():
    class CodeEnumExample(kernel.CodeEnum):
        A = 0
        B = 1

    assert CodeEnumExample(1) == CodeEnumExample.B
    assert CodeEnumExample(0) == CodeEnumExample.A
    assert CodeEnumExample["B"] == CodeEnumExample.B
    assert CodeEnumExample("B") == CodeEnumExample.B
    assert CodeEnumExample("A") == CodeEnumExample.A
    CodeEnumExample.A.assert_equals(CodeEnumExample.A)
    try:
        CodeEnumExample.A.assert_equals(CodeEnumExample.B)
        ok_(False)
    except AssertionError:
        ...
    assert hex(CodeEnumExample.A) == "0x0"

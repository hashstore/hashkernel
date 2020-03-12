import abc
import codecs
import enum
import json
import sys
from datetime import date, datetime
from enum import EnumMeta
from functools import total_ordering
from inspect import isclass, isfunction, ismodule
from pathlib import Path
from types import ModuleType
from typing import (
    IO,
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from hashkernel.typings import is_from_typing_module

_GLOBAL_REF = "__global_ref__"

ENCODING_USED = "utf-8"


class Primitive:
    pass


class BitMask:
    """
    Continues rage of bits

    >>> for i in range(8): print(str(BitMask(i)))
    0 mask:00000001 inverse:11111110
    1 mask:00000010 inverse:11111101
    2 mask:00000100 inverse:11111011
    3 mask:00001000 inverse:11110111
    4 mask:00010000 inverse:11101111
    5 mask:00100000 inverse:11011111
    6 mask:01000000 inverse:10111111
    7 mask:10000000 inverse:01111111

    >>> for i in range(7): print(str(BitMask(i,2)))
    0 mask:00000011 inverse:11111100
    1 mask:00000110 inverse:11111001
    2 mask:00001100 inverse:11110011
    3 mask:00011000 inverse:11100111
    4 mask:00110000 inverse:11001111
    5 mask:01100000 inverse:10011111
    6 mask:11000000 inverse:00111111

    >>> def pb(n): print(f'{n:08b}')
    >>> b23 = BitMask(2,2)
    >>> b456 = BitMask(4,3)
    >>> n=b23.update(0,7)
    >>> pb(n)
    00001100
    >>> n=b456.update(n,5)
    >>> pb(n)
    01011100
    >>> b23.extract(n)
    3
    >>> b456.extract(n)
    5
    >>> pb(b23.clear(n))
    01010000
    >>> pb(b456.set(n))
    01111100
    >>> pb(BitMask.update_all(0, (b23, 3), (b456, 5)))
    01011100
    """

    def __init__(self, start, size=1):
        bits = range(start, start + size)
        mask = 0
        for p in bits:
            assert 0 <= p < 8
            mask |= 1 << p
        self.position = start
        self.mask = mask
        self.inverse = mask ^ 0xFF

    def extract(self, i: int):
        return (i & self.mask) >> self.position

    def clear(self, i: int):
        return i & self.inverse

    def set(self, i: int):
        return i | self.mask

    def update(self, i: int, v: Any) -> int:
        if not isinstance(v, int):
            v = int(v)
        return self.clear(i) | ((v << self.position) & self.mask)

    @staticmethod
    def update_all(i: int, *mask_values: Tuple["BitMask", Any]) -> int:
        for mask, v in mask_values:
            i = mask.update(i, v)
        return i

    def __str__(self):
        return f"{self.position} mask:{self.mask:08b} inverse:{self.inverse:08b}"


def is_primitive(cls: Any) -> bool:
    """
    >>> is_primitive(Any)
    False
    >>> is_primitive(int)
    True
    >>> is_primitive(tuple)
    False
    """
    return isinstance(cls, type) and issubclass(
        cls, (int, float, bool, bytes, str, date, datetime, Primitive)
    )


def not_zero_len(v):
    return len(v) != 0


def quict(**kwargs):
    """
    Create dictionary from `kwargs`

    >>> quict(a=3, x="a")
    {'a': 3, 'x': 'a'}
    """
    r = {}
    r.update(**kwargs)
    return r


def identity(v):
    """
    >>> identity(None)
    >>> identity(5)
    5
    >>>
    """
    return v


def from_camel_case_to_underscores(s: str) -> str:
    """
    >>> from_camel_case_to_underscores('CamelCase')
    'camel_case'
    """
    return "".join(map(lambda c: c if c.islower() else "_" + c.lower(), s)).strip("_")


def lazy_factory(cls, factory):
    return lambda v: v if issubclass(type(v), cls) else factory(v)


def exception_message(e=None):
    if e is None:
        e = sys.exc_info()[1]
    return str(e)


def reraise_with_msg(msg, exception=None):
    if exception is None:
        exception = sys.exc_info()[1]
    etype = type(exception)
    new_msg = exception_message(exception) + "\n" + msg
    try:
        new_exception = etype(new_msg)
    except:
        new_exception = ValueError(new_msg)
    traceback = sys.exc_info()[2]
    raise new_exception.with_traceback(traceback)


def ensure_bytes(s: Any) -> bytes:
    """
    >>> ensure_bytes(b's')
    b's'
    >>> ensure_bytes('s')
    b's'
    >>> ensure_bytes(5)
    b'5'
    """
    if isinstance(s, bytes):
        return s
    if not isinstance(s, str):
        s = str(s)
    return utf8_encode(s)


def utf8_encode(s: str) -> bytes:
    return s.encode(ENCODING_USED)


def ensure_string(s: Any) -> str:
    """
    >>> ensure_string('s')
    's'
    >>> ensure_string(b's')
    's'
    >>> ensure_string(5)
    '5'
    """
    if isinstance(s, str):
        return s
    if isinstance(s, bytes):
        return utf8_decode(s)
    return str(s)


def utf8_decode(s: bytes) -> str:
    return s.decode(ENCODING_USED)


utf8_reader = codecs.getreader(ENCODING_USED)


def mix_in(
    source: type,
    target: type,
    should_copy: Optional[Callable[[str, bool], bool]] = None,
) -> List[str]:
    """
    Copy all defined functions from mixin into target. It could be
    usefull when you cannot inherit from mixin because incompatible
    metaclass. It does not copy abstract functions. If `source` is
    `ABCMeta`, will register `target` with it.

    Returns list of copied methods.
    """
    mixed_in_methods = []
    try:
        abstract_methods = source.__abstractmethods__  # type:ignore
    except AttributeError:
        abstract_methods = set()
    target_members = dir(target)
    for n in dir(source):
        fn = getattr(source, n)
        if isfunction(fn) and n not in abstract_methods:
            already_exists = n not in target_members
            if should_copy is None or should_copy(n, already_exists):
                setattr(target, n, fn)
                mixed_in_methods.append(n)
    if isinstance(source, abc.ABCMeta):
        source.register(target)
    return mixed_in_methods


EnsureItT = TypeVar("EnsureItT", bound="EnsureIt")


class EnsureIt:
    @classmethod
    def __factory__(cls):
        return cls

    @classmethod
    def ensure_it(cls: Type[EnsureItT], o: Any) -> EnsureItT:
        if isinstance(o, cls):
            return o
        return cls.__factory__()(o)

    @classmethod
    def ensure_it_or_none(cls: Type[EnsureItT], o: Any) -> Optional[EnsureItT]:
        if o is None:
            return o
        return cls.ensure_it(o)


class Str2Bytes:
    def __bytes__(self) -> bytes:
        return str(self).encode(ENCODING_USED)


class Stringable(Str2Bytes):
    """
    Marker to inform `json_encode()` to use `str(o)` to
    serialize in json. Also assumes that any implementing
    class has constructor that recreate same object from
    it's string representation as single parameter.
    """

    def __repr__(self) -> str:
        return f"{type(self).__name__}({repr(str(self))})"


class StrigableFactory(EnsureIt, Stringable):
    """
    Allow string to be converted into new instance.

    Name of the classes has to be registered with subclass.
    """

    subcls_mapping: ClassVar[Dict[type, Dict[str, type]]] = {}
    inverse_mapping: ClassVar[Dict[type, str]] = {}

    def __str__(self):
        cls = type(self)
        return cls.inverse_mapping[cls]

    @classmethod
    def register(cls, name, subcls):
        if subcls in cls.inverse_mapping:
            raise ValueError(f"cannot register class twice: {subcls} {name}")
        if cls not in cls.subcls_mapping:
            cls.subcls_mapping[cls] = {}
        cls.subcls_mapping[cls][name] = subcls
        cls.inverse_mapping[subcls] = name

    @classmethod
    def __factory__(cls):
        return lambda s: cls.subcls_mapping[cls][s]()


class Integerable(EnsureIt):
    """
    Marker to inform `json_encode()` to use `int(o)` to
    serialize in json. Also assumes that any implementing
    class has constructor that recreate same object from
    it's integer number as single parameter.
    """

    def __repr__(self) -> str:
        return f"{type(self).__name__}({int(self)})"

    def __int__(self) -> int:
        pass


class StrKeyMixin:
    """
    mixin for immutable objects to implement
    `__hash__()`, `__eq__()`, `__ne__()`.

    Implementation of methods expect super class to implement
    `__str__()` and object itself to be immutable (`str(obj)`
    expected to return same value thru the life of object)


    >>> class X(StrKeyMixin):
    ...     def __init__(self, x):
    ...         self.x = x
    ...
    ...     def __str__(self):
    ...         return self.x
    ...
    >>> a = X('A')
    >>> a != X('B')
    True
    >>> X('A') == X('B')
    False
    >>> a == X('A')
    True
    >>> a == 'A'
    False
    >>> a != X('A')
    False
    >>> hash(a) == hash(X('A'))
    True
    >>> hash(a) != hash(X('B'))
    True
    """

    def __cached_str(self) -> str:
        if not (hasattr(self, "_str")):
            self._str = str(self)
        return self._str

    def __hash__(self):
        if not (hasattr(self, "_hash")):
            self._hash = hash(self.__cached_str())
        return self._hash

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.__cached_str() == other.__cached_str()

    def __ne__(self, other):
        return not self.__eq__(other)


class Jsonable(EnsureIt):
    """
    Marker to inform `json_encode()` to use `to_json(o)` to
    serialize in json

    """

    def __to_json__(self):
        raise AssertionError("need to be implemented")

    def __bytes__(self):
        return utf8_encode(str(self))

    def __str__(self):
        return json_encode(to_json(self))

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return str(self) == str(other)

    def __ne__(self, other):
        return not (self.__eq__(other))


def to_json(v: Any) -> Any:
    if hasattr(v, "__to_json__"):
        return v.__to_json__()
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Stringable):
        return str(v)
    if isinstance(v, Integerable):
        return int(v)
    if isinstance(v, dict):
        return {str(k): to_json(v) for k, v in v.items()}
    if isinstance(v, (list, tuple)):
        return [to_json(v) for v in v]
    if isinstance(v, (int, bool, float, str)):
        return v
    if v is None:
        return v
    raise NotImplementedError(f"No conversion defined for: {v!r}")


def load_jsonable(path: Union[Path, str], cls: type) -> Any:
    with Path(path).open(mode="rb") as fp:
        return read_jsonable(fp, cls)


def dump_jsonable(path: Union[Path, str], v: Any):
    with Path(path).open(mode="wb") as fp:
        return write_jsonable(fp, v)


def read_jsonable(fp: IO[bytes], cls: type, n: int = -1) -> Any:
    return cls(json_decode(utf8_decode(fp.read(n))))


def write_jsonable(fp: IO[bytes], v: Any):
    return fp.write(utf8_encode(json_encode(to_json(v))))


def to_tuple(v: Any) -> tuple:
    if hasattr(v, "__to_tuple__"):
        return v.__to_tuple__()
    return tuple(v)


def to_dict(v: Any) -> Dict[str, Any]:
    if hasattr(v, "__to_dict__"):
        return v.__to_dict__()
    raise NotImplementedError()


class _StringableEncoder(json.JSONEncoder):
    def __init__(self):
        json.JSONEncoder.__init__(self, sort_keys=True)

    def default(self, v):
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, Integerable):
            return int(v)
        if isinstance(v, Stringable):
            return str(v)
        if hasattr(v, "__to_json__"):
            return v.__to_json__()
        return json.JSONEncoder.default(self, v)


json_encode = _StringableEncoder().encode


def load_json_file(file_path: str):
    return json.load(open(file_path))


def json_decode(text: str):
    try:
        return json.loads(text)
    except:
        reraise_with_msg(f"text={repr(text)}")


class GlobalRef(Stringable, EnsureIt, StrKeyMixin):
    """
    >>> ref = GlobalRef('hashkernel:GlobalRef')
    >>> ref
    GlobalRef('hashkernel:GlobalRef')
    >>> ref.get_instance().__name__
    'GlobalRef'
    >>> ref.module_only()
    False
    >>> ref.get_module().__name__
    'hashkernel'
    >>> GlobalRef(GlobalRef)
    GlobalRef('hashkernel:GlobalRef')
    >>> GlobalRef(GlobalRef).get_instance()
    <class 'hashkernel.GlobalRef'>
    >>> uref = GlobalRef('hashkernel:')
    >>> uref.module_only()
    True
    >>> uref.get_module().__name__
    'hashkernel'
    >>> uref = GlobalRef('hashkernel')
    >>> uref.module_only()
    True
    >>> uref.get_module().__name__
    'hashkernel'
    >>> uref = GlobalRef(uref.get_module())
    >>> uref.module_only()
    True
    >>> uref.get_module().__name__
    'hashkernel'
    >>> GlobalRef("abc]")
    Traceback (most recent call last):
    ...
    ValueError: not enough values to unpack (expected 2, got 1)
    abc]


    """

    def __init__(self, s: Any, item: Optional[str] = None) -> None:
        self.item = item
        if hasattr(s, _GLOBAL_REF):
            that = getattr(s, _GLOBAL_REF)
            self.module, self.name, self.item = (that.module, that.name, that.item)
        elif ismodule(s):
            self.module, self.name = s.__name__, ""
        elif isclass(s) or isfunction(s):
            self.module, self.name = s.__module__, s.__name__
        else:
            try:
                if s[-1] == "]":
                    s, self.item = s[:-1].split("[")
            except:
                reraise_with_msg(f"{s}")
            split = s.split(":")
            if len(split) == 1:
                if not (split[0]):
                    raise AssertionError(f"is {repr(s)} empty?")
                split.append("")
            elif len(split) != 2:
                raise AssertionError(f"too many ':' in: {repr(s)}")
            self.module, self.name = split

    def __str__(self):
        item = "" if self.item is None else f"[{self.item}]"
        return f"{self.module}:{self.name}{item}"

    def get_module(self) -> ModuleType:
        return __import__(self.module, fromlist=[""])

    def module_only(self) -> bool:
        return not (self.name)

    def get_instance(self) -> Any:
        if self.module_only():
            raise AssertionError(f"{repr(self)}.get_module() only")
        attr = getattr(self.get_module(), self.name)
        if self.item is None:
            return attr
        else:
            return attr[self.item]


def ensure_module(o: Union[str, GlobalRef, ModuleType]) -> ModuleType:
    """
    >>> m = ensure_module('hashkernel')
    >>> m.__name__
    'hashkernel'
    >>> ensure_module(m).__name__
    'hashkernel'
    >>> ensure_module(GlobalRef(GlobalRef))
    Traceback (most recent call last):
    ...
    ValueError: ref:hashkernel:GlobalRef has to be module
    """
    if isinstance(o, ModuleType):
        return o
    ref = GlobalRef.ensure_it(o)
    if not ref.module_only():
        raise ValueError(f"ref:{ref} has to be module")
    return ref.get_module()


CodeEnumT = TypeVar("CodeEnumT", bound="CodeEnum")


class CodeEnum(Stringable, enum.Enum):
    """
    >>> class CodeEnumExample(CodeEnum):
    ...     A = 0
    ...     B = 1, "some important help message"
    ...
    >>> int(CodeEnumExample.A)
    0
    >>> CodeEnumExample(0)
    <CodeEnumExample.A: 0>
    >>> CodeEnumExample.B.__doc__
    'some important help message'
    >>> CodeEnumExample.find_by_code(1)
    <CodeEnumExample.B: 1>
    >>> CodeEnumExample(1)
    <CodeEnumExample.B: 1>
    >>> CodeEnumExample("B")
    <CodeEnumExample.B: 1>
    >>> CodeEnumExample["B"]
    <CodeEnumExample.B: 1>
    >>> CodeEnumExample[1]
    Traceback (most recent call last):
    ...
    KeyError: 1
    """

    def __init__(self, code: int, doc: str = "") -> None:
        self.__doc__ = doc
        self.code = code
        by_code: Dict[int, CodeEnumT] = type(self)._value2member_map_  # type: ignore
        if code in by_code:
            raise TypeError(f"duplicate code: {self} = {code}")
        by_code[code] = self

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, int):
            return cls.find_by_code(value)  # pragma: no cover
        else:
            return cls[value]

    @classmethod
    def find_by_code(cls: Type[CodeEnumT], code: int) -> CodeEnumT:
        return cls._value2member_map_[code]  # type: ignore

    def assert_equals(self, type):
        if type != self:
            raise AssertionError(f"has to be {self} and not {type}")

    def __int__(self):
        return self.code

    def __index__(self):
        return self.code

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"<{type(self).__name__}.{self.name}: {self.code}>"


class MetaCodeEnumExtended(EnumMeta):
    """

    """

    @classmethod
    def __prepare__(metacls, name, bases, enums=None, **kargs):
        """
        Generates the class's namespace.
        @param enums Iterable of `enum.Enum` classes to include in the new class.  Conflicts will
            be resolved by overriding existing values defined by Enums earlier in the iterable with
            values defined by Enums later in the iterable.
        """
        if enums is None:
            raise ValueError(
                "Class keyword argument `enums` must be defined to use this metaclass."
            )
        ret = super().__prepare__(name, bases, **kargs)
        for enm in enums:
            for item in enm:
                ret[item.name] = item.value  # Throws `TypeError` if conflict.
        return ret

    def __new__(metacls, name, bases, namespace, **kargs):
        return super().__new__(metacls, name, bases, namespace)
        # DO NOT send "**kargs" to "type.__new__".  It won't catch them and
        # you'll get a "TypeError: type() takes 1 or 3 arguments" exception.

    def __init__(cls, name, bases, namespace, **kargs):
        super().__init__(name, bases, namespace)
        # DO NOT send "**kargs" to "type.__init__" in Python 3.5 and older.  You'll get a
        # "TypeError: type.__init__() takes no keyword arguments" exception.


class LogicRegistry:
    """
    Associate logic with CodeEnum using code as key
    """

    def __init__(self):
        self.logic_by_code: Dict[int, Callable[..., Any]] = {}

    def add_all(self, registry: "LogicRegistry") -> "LogicRegistry":
        for k, v in registry.logic_by_code.items():
            assert k not in self.logic_by_code
            self.logic_by_code[k] = v
        return self

    def add(self, e: CodeEnumT):
        def decorate(fn):
            assert e.code not in self.logic_by_code
            self.logic_by_code[e.code] = fn
            return fn

        return decorate

    def get(self, e: Union[CodeEnumT, int]) -> Callable[..., Any]:
        return self.logic_by_code[self.code(e)]

    def code(self, e: Union[CodeEnumT, int]) -> int:
        return e if isinstance(e, int) else e.code

    def has(self, e: Union[CodeEnumT, int]):
        return self.code(e) in self.logic_by_code


def delegate_factory(cls: type, delegate_attrs: Iterable[str]) -> Callable[[Any], Any]:
    """
    Create factory function that searches object `o` for `delegate_attrs`
    and check is any of these attributes have `cls` type. If no such
    attributes found it calls `cls(o)` to cast it into desired type.

    >>> class Z:
    ...     def __int__(self):
    ...         return 7
    ...
    >>> x = Z()
    >>> x.a = 5
    >>> y = Z()
    >>> y.b = 3
    >>> q = Z()
    >>> q.b = 'str'
    >>> z = Z()
    >>> factory = delegate_factory(int, ["a","b"])
    >>> factory(x)
    5
    >>> factory(y)
    3
    >>> factory(q)
    7
    >>> factory(z)
    7
    """

    def cls_factory(o: Any) -> Any:
        for posible_delegate in delegate_attrs:
            if hasattr(o, posible_delegate):
                candidate_obj = getattr(o, posible_delegate)
                if isinstance(candidate_obj, cls):
                    return candidate_obj
        return cls(o)

    return cls_factory


class DictLike(Mapping[str, Any]):
    """
    Allow query object public attributes like dictionary

    >>> class X:
    ...     pass
    ...
    >>> x=X()
    >>> x.q = 5
    >>> dl = DictLike(x)
    >>> list(dl)
    ['q']
    >>> dl['q']
    5

    """

    def __init__(self, o):
        self.o = o
        self.members = list(k for k in dir(o) if k[:1] != "_")

    def __contains__(self, item):
        return hasattr(self.o, item)

    def __getitem__(self, item):
        return getattr(self.o, item)

    def __iter__(self):
        return iter(self.members)

    def __len__(self):
        return len(self.members)


class ScaleHelper:
    scale_fn: Callable[[int], Any]
    masks: List[int]
    size: int
    scaled_values: List[Any]
    cache: List[Any]

    def __init__(self, scale_fn: Callable[[int], Any], bit_size: int):
        self.scale_fn = scale_fn  # type:ignore
        # TODO: remove ignore when fixed https://github.com/python/mypy/issues/708
        self.size = 2 ** bit_size
        self.cache = [None for _ in range(self.size)]
        self.scaled_values = [scale_fn(i) for i in range(self.size)]
        self.masks = [1 << i for i in range(bit_size - 1, -1, -1)]

    def build_cls(self, cls: Type["Scaling"], idx: int):
        instance = self.cache[idx]
        if instance is None:
            instance = super(Scaling, cls).__new__(cls)
            instance.idx = idx
            self.cache[idx] = instance
        return instance


@total_ordering
class Scaling(Integerable):
    """

    Scaling - to map range of values to discrete points and store
    it as integer index between `0` and `2 ** bit_size`.

    Order of scaled value should be that same as
    index otherwise binary search will not work properly.

    By default initialized with power of 5 scaling and them
    to 4 bit integer `idx`.

    >>> Scaling(0).value()
    1
    >>> Scaling(1).value()
    5
    >>> Scaling(2).value()
    25

    >>> t=Scaling.search(26)
    >>> t.idx
    3

    >>> class PowerOf2(Scaling):
    ...    @staticmethod
    ...    def __new_scale_helper__():
    ...        return ScaleHelper(lambda i: 2 ** i, 3)
    ...
    >>> [sc.value() for sc in PowerOf2.all()]
    [1, 2, 4, 8, 16, 32, 64, 128]

    Because `Scaling` lazily caches all its instances.
    Avoid using if scaling range is too big. `all()` will force all
    possible instances into cache.

    >>> PowerOf2.search(50)
    PowerOf2(6)

    """

    idx: int

    helpers: ClassVar[Dict[type, ScaleHelper]] = {}

    @staticmethod
    def __new_scale_helper__():
        """
        Helper factory

        Need to be overwritten by subclass. Default implementaion
        create power of 5 scaler with range that fits into 4 bits
        """
        return ScaleHelper(lambda i: 5 ** i, 4)

    @staticmethod
    def __new__(cls, idx: int):
        return cls.helper().build_cls(cls, idx)

    @classmethod
    def helper(cls) -> ScaleHelper:
        if cls not in cls.helpers:
            cls.helpers[cls] = cls.__new_scale_helper__()
        helper = cls.helpers[cls]
        return helper

    @classmethod
    def search(cls, value: Any) -> "Scaling":
        "Binary search to the bucket in the scale"
        idx = 0
        h = cls.helper()
        for m in h.masks:
            c = h.scaled_values[idx + m - 1]
            if value > c:
                idx += m
        return cls(idx)

    def value(self):
        return self.helper().scaled_values[self.idx]

    def __eq__(self, other):
        return self.idx == other.idx

    def __lt__(self, other):
        return self.idx < other.idx

    def __int__(self):
        return self.idx

    @classmethod
    def all(cls):
        """
        All posible values
        """
        return (cls(i) for i in range(cls.size()))

    @classmethod
    def size(cls):
        """
        All posible values
        """
        return cls.helper().size

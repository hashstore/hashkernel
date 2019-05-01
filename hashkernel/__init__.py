import enum
from types import ModuleType
import abc
import json
from typing import (Any, List, Type, TypeVar, Optional, Union, Dict,
                    Callable)
from inspect import isfunction, isclass, ismodule
import codecs
from datetime import datetime, date
from dateutil.parser import parse as dt_parse
from hashkernel.typings import is_from_typing_module
import sys

_GLOBAL_REF = '__global_ref__'

ENCODING_USED = 'utf-8'


def not_zero_len(v):
    return len(v)!=0


def quict(**kwargs):
    """
    Shortcut to created dictionary with `kwargs` fasion

    >>> quict(a=3,x="a")
    {'a': 3, 'x': 'a'}
    """
    r = {}
    r.update(**kwargs)
    return r


class DictLike:
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

    def __contains__(self, item):
        return hasattr(self.o, item)

    def __getitem__(self, item):
        return getattr(self.o, item)

    def __iter__(self):
        return iter(k for k in dir(self.o) if k[:1] != '_' )

def identity(v):
    """
    >>> identity(None)
    >>> identity(5)
    5
    >>>
    """
    return v

def first_elem(t):
    """
    >>> first_elem([5])
    5
    >>> first_elem((6,))
    6
    >>> first_elem([])
    Traceback (most recent call last):
    ...
    IndexError: list index out of range

    """
    return t[0]


def from_camel_case_to_underscores(s:str)->str:
    '''
    >>> from_camel_case_to_underscores('CamelCase')
    'camel_case'
    '''
    return ''.join(map(
        lambda c: c if c.islower() else '_' + c.lower(), s
    )).strip('_')


def lazy_factory(cls, factory):
    return lambda v: v if issubclass(type(v), cls) else factory(v)


def exception_message(e = None):
    if e is None:
        e = sys.exc_info()[1]
    return str(e)


def reraise_with_msg(msg, exception=None):
    if exception is None:
        exception = sys.exc_info()[1]
    etype = type(exception)
    try:
        new_exception = etype(exception_message(exception) + '\n'+ msg)
    except:
        new_exception = ValueError(exception_message(exception) + '\n'+ msg)
    traceback = sys.exc_info()[2]
    raise new_exception.with_traceback(traceback)


def ensure_bytes(s: Any)->bytes:
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


def utf8_encode(s:str)->bytes:
    return s.encode(ENCODING_USED)


def ensure_string(s: Any)->str:
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


def utf8_decode(s:bytes)->str:
    return s.decode(ENCODING_USED)


utf8_reader = codecs.getreader(ENCODING_USED)


def mix_in(source:type,
           target:type,
           should_copy:Optional[Callable[[str, bool],bool]] = None
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
        abstract_methods = source.__abstractmethods__ # type:ignore
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


class EnsureIt:

    @classmethod
    def factory(cls):
        return cls

    @classmethod
    def ensure_it(cls, o):
        if isinstance(o, cls):
            return o
        return cls.factory()(o)

    @classmethod
    def ensure_it_or_none(cls, o):
        if o is None:
            return o
        return cls.ensure_it(o)


class Str2Bytes:
    def __bytes__(self)->bytes:
        return str(self).encode(ENCODING_USED)


class Stringable(Str2Bytes):
    '''
    Marker to inform json_encoder to use `str(o)` to
    serialize in json. Also assumes that any implementing
    class has constructor that recreate same object from
    it's string representation as single parameter.
    '''
    def __repr__(self)->str:
        return f'{type(self).__name__}({repr(str(self))})'


class StrKeyMixin:
    '''
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
    '''
    def __cached_str(self) -> str:
        if not(hasattr(self, '_str')):
            self._str = str(self)
        return self._str

    def __hash__(self):
        if not(hasattr(self, '_hash')):
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
    Marker to inform json_encoder to use `to_json(o)` to
    serialize in json

    """

    def __to_json__(self):
        raise AssertionError('need to be implemented')

    def __bytes__(self):
        return utf8_encode(str(self))

    def __str__(self):
        return json_encode(to_json(self))

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return str(self) == str(other)

    def __ne__(self, other):
        return not(self.__eq__(other))


def to_json(v:Any)->Any:
    if hasattr(v, '__to_json__'):
        return v.__to_json__()
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Stringable):
        return str(v)
    if isinstance(v, (int,bool,float,str,dict,list,tuple)):
        return v
    raise NotImplementedError()


def to_tuple(v:Any)->tuple:
    if hasattr(v, '__to_tuple__'):
        return v.__to_tuple__()
    return tuple(v)


def to_dict(v:Any)->Dict[str,Any]:
    if hasattr(v, '__to_dict__'):
        return v.__to_dict__()
    raise NotImplementedError()


class _StringableEncoder(json.JSONEncoder):
    def __init__(self):
        json.JSONEncoder.__init__(self, sort_keys=True)

    def default(self, v):
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, Stringable):
            return str(v)
        if hasattr(v, '__to_json__'):
            return v.__to_json__()
        return json.JSONEncoder.default(self, v)


json_encode = _StringableEncoder().encode


def load_json_file(file_path: str):
    return json.load(open(file_path))


def json_decode(text: str):
    try:
        return json.loads(text)
    except:
        reraise_with_msg(f'text={repr(text)}')


class GlobalRef(Stringable, EnsureIt, StrKeyMixin):
    '''
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
    '''
    def __init__(self, s: Any, item: Optional[str] = None)->None:
        self.item = item
        if hasattr(s, _GLOBAL_REF):
            that = getattr(s, _GLOBAL_REF)
            self.module, self.name, self.item = ( that.module, that.name, that.item)
        elif ismodule(s):
                self.module,self.name = s.__name__,''
        elif isclass(s) or isfunction(s):
            self.module, self.name = s.__module__, s.__name__
        else:
            try:
                if s[-1] == ']' :
                    s, self.item = s[:-1].split('[')
            except:
                reraise_with_msg(f'{s}')
            split = s.split(':')
            if len(split) == 1:
                if not(split[0]):
                    raise AssertionError(f'is {repr(s)} empty?')
                split.append('')
            elif len(split) != 2:
                raise AssertionError(f"too many ':' in: {repr(s)}")
            self.module, self.name = split

    def __str__(self):
        item = '' if self.item is None else f'[{self.item}]'
        return f'{self.module}:{self.name}{item}'

    def get_module(self)->ModuleType:
        return __import__(self.module, fromlist=['', ])

    def module_only(self)->bool:
        return not(self.name)

    def get_instance(self)->Any:
        if self.module_only():
            raise AssertionError(f'{repr(self)}.get_module() only')
        attr = getattr(self.get_module(), self.name)
        if self.item is None:
            return attr
        else:
            return attr[self.item]


def ensure_module(o:Union[str,GlobalRef,ModuleType]) -> ModuleType:
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
        raise ValueError(f'ref:{ref} has to be module')
    return ref.get_module()

    
CodeEnumT = TypeVar('CodeEnumT', bound='CodeEnum')


class CodeEnum(Stringable, enum.Enum):

    def __init__(self, code:int) -> None:
        self.code = code
        type(self)._value2member_map_[code] = self # type: ignore

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, int):
            return cls.find_by_code(value) # pragma: no cover
        else:
            return cls[value]

    @classmethod
    def find_by_code(cls : Type[CodeEnumT], code:int) -> CodeEnumT:
        return cls._value2member_map_[code] # type: ignore

    def assert_equals(self, type):
        if type != self:
            raise AssertionError(f"has to be {self} and not {type}")

    def __index__(self):
        return self.code

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'<{type(self).__name__}.{self.name}: {self.code}>'


class Conversion(enum.IntEnum):
    TO_JSON = -1
    TO_OBJECT = 1


class ClassRef(Stringable, StrKeyMixin, EnsureIt):
    """
    >>> crint=ClassRef('int')
    >>> str(crint)
    'int'
    >>> crint.convert(5, Conversion.TO_JSON)
    5
    >>> crint.convert('3', Conversion.TO_OBJECT)
    3
    >>> crint = ClassRef(int)
    >>> crint.convert(5, Conversion.TO_JSON)
    5
    >>> crint.convert('3', Conversion.TO_OBJECT)
    3
    >>> crint.matches(3)
    True
    >>> crint.matches('3')
    False
    >>> crgr=ClassRef(GlobalRef(GlobalRef))
    >>> crgr.matches(GlobalRef(GlobalRef))
    True
    """

    def __init__(self,
                 cls_or_str: Union[type, GlobalRef, str])->None:
        if isinstance(cls_or_str, str):
            if ':' not in cls_or_str:
                cls_or_str = 'builtins:'+cls_or_str
            cls_or_str = GlobalRef(cls_or_str).get_instance()
        elif isinstance(cls_or_str, GlobalRef):
            cls_or_str = cls_or_str.get_instance()

        self.cls = cls_or_str
        self.primitive = self.cls.__module__ == 'builtins'
        if self.cls == Any:
            self._from_json = identity
        elif self.cls is date:
            self.primitive = True
            self._from_json = lazy_factory(
                self.cls, lambda v: dt_parse(v).date())
        elif self.cls is datetime:
            self.primitive = True
            self._from_json = lazy_factory(
                self.cls, lambda v: dt_parse(v))
        elif hasattr(self.cls, '__args__'):
            self._from_json = identity
        elif isinstance(self.cls, type):
            self._from_json = lazy_factory(self.cls, self.cls)
        else:
            self._from_json = identity

    def matches(self, v):
        return self.cls == Any or isinstance(v, self.cls)

    def convert(self, v: Any, direction: Conversion)->Any:
        try:
            if direction == Conversion.TO_OBJECT:
                return self._from_json(v)
            else:
                return to_json(v)
        except:
            reraise_with_msg(f'{self.cls} {v}')

    def __str__(self):
        if self.cls.__module__ == 'builtins':
            return self.cls.__name__
        elif is_from_typing_module(self.cls):
            return str(self.cls)
        return str(GlobalRef(self.cls))


class Template(type):
    def __init__(cls, name, bases, dct):
        if _GLOBAL_REF not in dct:
            cls.__cache__ = {}

    def __getitem__(cls, item):
        item_cref = ClassRef.ensure_it(item)
        k = str(item_cref)
        if k in cls.__cache__:
            return cls.__cache__[k]
        global_ref = GlobalRef(cls, str(item_cref))
        class Klass(cls):
            __item_cref__ = item_cref
            __global_ref__ = global_ref
        cls.__cache__[k]= Klass
        return Klass


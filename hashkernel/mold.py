import abc
from datetime import date, datetime
from enum import IntEnum
from inspect import getfullargspec
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from dateutil.parser import parse as dt_parse

from hashkernel import (
    DictLike,
    EnsureIt,
    GlobalRef,
    Jsonable,
    Stringable,
    StrKeyMixin,
    delegate_factory,
    identity,
    is_from_typing_module,
    is_primitive,
    json_decode,
    json_encode,
    lazy_factory,
    reraise_with_msg,
    to_json,
)
from hashkernel.docs import DocStringTemplate, GroupOfVariables, VariableDocEntry
from hashkernel.typings import (
    OnlyAnnotatedProperties,
    get_args,
    get_attr_hints,
    is_dict,
    is_list,
    is_optional,
    is_tuple,)


class Conversion(IntEnum):
    """
    >>> def flator_object(c):
    ...     return (c.need_flator(), c.to_object())
    ...
    >>> flator_object(Conversion.DEFLATE)
    (True, False)
    >>> flator_object(Conversion.INFLATE)
    (True, True)
    >>> flator_object(Conversion.TO_JSON)
    (False, False)
    >>> flator_object(Conversion.TO_OBJECT)
    (False, True)

    """

    DEFLATE = -2
    TO_JSON = -1
    TO_OBJECT = 1
    INFLATE = 2

    def need_flator(self):
        return abs(self.value) == 2

    def to_object(self):
        return self.value > 0


class Flator(metaclass=abc.ABCMeta):
    """
    Inflator/Deflator

    """

    @abc.abstractmethod
    def is_applied(self, cls: type) -> bool:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def inflate(self, k: str, cls:type) -> Any:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def deflate(self, data: Any) -> str:
        raise NotImplementedError("subclasses must override")


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
    >>> crgr=ClassRef(GlobalRef)
    >>> crgr.matches(GlobalRef(GlobalRef))
    True
    >>> crgr=ClassRef(GlobalRef(GlobalRef))
    >>> crgr.matches(GlobalRef(GlobalRef))
    True
    >>>
    """

    def __init__(self, cls_or_str):
        if isinstance(cls_or_str, str):
            if ":" not in cls_or_str:
                cls_or_str = "builtins:" + cls_or_str
            cls_or_str = GlobalRef(cls_or_str).get_instance()
        if isinstance(cls_or_str, GlobalRef):
            cls_or_str = cls_or_str.get_instance()
        self.cls = cls_or_str
        self.primitive = is_primitive(self.cls)
        if self.cls == Any:
            self._from_json = identity
        elif self.cls is date:
            self._from_json = lazy_factory(self.cls, lambda v: dt_parse(v).date())
        elif self.cls is datetime:
            self._from_json = lazy_factory(self.cls, lambda v: dt_parse(v))
        elif hasattr(self.cls, "__args__") or not (isinstance(self.cls, type)):
            self._from_json = identity
        else:
            self._from_json = lazy_factory(self.cls, self.cls)

    def matches(self, v):
        return self.cls == Any or isinstance(v, self.cls)

    def convert(
        self, v: Any, direction: Conversion, flator: Optional[Flator] = None
    ) -> Any:
        try:
            if (
                direction.need_flator()
                and flator is not None
                and flator.is_applied(self.cls)
            ):
                if direction.to_object():
                    if isinstance(v, str):
                        return flator.inflate(v, self.cls)
                else:
                    if isinstance(v, self.cls):
                        return flator.deflate(v)
            if direction.to_object():
                return self._from_json(v)
            else:
                return to_json(v)
        except:
            reraise_with_msg(f"{self.cls} {v}")

    def __str__(self):
        if self.cls.__module__ == "builtins":
            return self.cls.__name__
        elif is_from_typing_module(self.cls):
            return str(self.cls)
        return str(GlobalRef(self.cls))


ATTRIBUTES = "Attributes"
RETURNS = "Returns"
ARGS = "Args"


class MoldConfig(OnlyAnnotatedProperties):
    omit_optional_null: bool = False


class ValueRequired(Exception):
    pass


class Typing(Stringable, EnsureIt):
    @classmethod
    def __factory__(cls):
        return typing_factory

    def __init__(self, val_cref, collection=False):
        self.val_cref = ClassRef.ensure_it(val_cref)
        self.collection = collection

    def convert(
        self, v: Any, direction: Conversion, flator: Optional[Flator] = None
    ) -> Any:
        return self.val_cref.convert(v, direction, flator)

    @classmethod
    def name(cls):
        return cls.__name__[:-6]

    def __str__(self):
        return f"{self.name()}[{self.val_cref}]"


class OptionalTyping(Typing):
    def validate(self, v):
        return v is None or self.val_cref.matches(v)

    def default(self):
        return None


class RequiredTyping(Typing):
    def validate(self, v):
        return self.val_cref.matches(v)

    def default(self):
        raise ValueRequired(f"no default for {str(self)}")


class DictTyping(Typing):
    def __init__(self, val_cref, key_cref):
        Typing.__init__(self, val_cref, collection=True)
        self.key_cref = ClassRef.ensure_it(key_cref)

    def convert(
        self, in_v: Any, direction: Conversion, flator: Optional[Flator] = None
    ) -> Dict[Any, Any]:
        return {
            self.key_cref.convert(k, direction): self.val_cref.convert(
                v, direction, flator
            )
            for k, v in in_v.items()
        }

    def validate(self, v):
        return isinstance(v, dict)

    def __str__(self):
        return f"{self.name()}[{self.key_cref},{self.val_cref}]"

    def default(self):
        return {}


class ListTyping(Typing):
    def __init__(self, val_cref):
        Typing.__init__(self, val_cref, collection=True)

    def convert(
        self, in_v: Any, direction: Conversion, flator: Optional[Flator] = None
    ) -> List[Any]:
        return [self.val_cref.convert(v, direction, flator) for v in in_v]

    def validate(self, v):
        return isinstance(v, list)

    def default(self):
        return []


class AttrEntry(EnsureIt, Stringable):
    """
    >>> AttrEntry('x:Required[hashkernel.tests:StringableExample]')
    AttrEntry('x:Required[hashkernel.tests:StringableExample]')
    >>> e = AttrEntry('x:Required[hashkernel.tests:StringableExample]="0"')
    >>> e.default
    StringableExample('0')
    >>> e
    AttrEntry('x:Required[hashkernel.tests:StringableExample]="0"')
    >>> AttrEntry(None)
    Traceback (most recent call last):
    ...
    AttributeError: 'NoneType' object has no attribute 'split'
    >>> AttrEntry('a')
    Traceback (most recent call last):
    ...
    ValueError: not enough values to unpack (expected 2, got 1)
    >>> AttrEntry('a:x')
    Traceback (most recent call last):
    ...
    AttributeError: Unrecognized typing: x
    >>> AttrEntry(5)
    Traceback (most recent call last):
    ...
    AttributeError: 'int' object has no attribute 'split'
    """

    def __init__(self, name, typing=None, default=None):
        self.default = default
        self._doc = None
        self.index = None
        default_s = None
        if typing is None:
            split = name.split("=", 1)
            if len(split) == 2:
                name, default_s = split
            name, typing = name.split(":", 1)
        self.name = name
        self.typing = typing_factory(typing)
        if default_s is not None:
            self.default = self.typing.convert(
                json_decode(default_s), Conversion.TO_OBJECT
            )

    def required(self):
        try:
            self.convert(None, Conversion.TO_OBJECT)
            return False
        except ValueRequired:
            return True

    def convert(
        self, v: Any, direction: Conversion, flator: Optional[Flator] = None
    ) -> Any:
        try:
            if direction.to_object():
                if v is None:
                    if self.default is not None:
                        return self.default
                    else:
                        return self.typing.default()
                else:
                    return self.typing.convert(v, direction, flator)
            else:
                if v is None:
                    return None
                else:
                    return self.typing.convert(v, direction, flator)
        except:
            reraise_with_msg(f"error in {self}")

    def validate(self, v: Any) -> bool:
        return self.typing.validate(v)

    def __str__(self):
        def_s = ""
        if self.default is not None:
            v = json_encode(self.typing.convert(self.default, Conversion.TO_JSON))
            def_s = f"={v}"
        return f"{self.name}:{self.typing}{def_s}"

    def is_optional(self):
        return isinstance(self.typing, OptionalTyping)


def typing_factory(o):
    """
    >>> req = typing_factory('Required[hashkernel.tests:StringableExample]')
    >>> req
    RequiredTyping('Required[hashkernel.tests:StringableExample]')
    >>> Typing.ensure_it(str(req))
    RequiredTyping('Required[hashkernel.tests:StringableExample]')
    >>> typing_factory(req)
    RequiredTyping('Required[hashkernel.tests:StringableExample]')
    >>> Typing.ensure_it('Dict[datetime:datetime,str]')
    DictTyping('Dict[datetime:datetime,str]')
    """

    if isinstance(o, Typing):
        return o
    if isinstance(o, str):
        if o[-1] == "]":
            typing_name, args_s = o[:-1].split("[", 1)
            args = args_s.split(",")
            typing_cls = globals()[typing_name + "Typing"]
            if issubclass(typing_cls, DictTyping):
                return typing_cls(args[1], args[0])
            elif len(args) != 1:
                raise AssertionError(f"len({args}) should be 1. input:{o}")
            else:
                return typing_cls(args[0])
        raise AttributeError(f"Unrecognized typing: {o}")
    else:
        args = get_args(o, [])
        if len(args) == 0:
            return RequiredTyping(o)
        elif is_optional(o, args):
            return OptionalTyping(args[0])
        elif is_list(o, args):
            return ListTyping(args[0])
        elif is_dict(o, args):
            return DictTyping(args[1], args[0])
        else:
            raise AssertionError(f"Unknown annotation: {o}")


SINGLE_RETURN_VALUE = "_"


class Mold(Jsonable):
    """
    >>> class X:
    ...    a: int
    ...    b: str = "zzz"
    ...    d: Optional[float]
    ...
    >>> Mold(X).__to_json__()
    ['a:Required[int]', 'b:Required[str]="zzz"', 'd:Optional[float]']
    >>> def fn(a:int, b:str)->int:
    ...     return 5
    ...
    >>> attr_envs = Mold(fn).__to_json__()
    >>> attr_envs
    ['a:Required[int]', 'b:Required[str]', 'return:Required[int]']
    >>> str(Mold(attr_envs))
    '["a:Required[int]", "b:Required[str]", "return:Required[int]"]'
    """

    def __init__(self, o=None, config=None):
        self.keys: List[str] = []
        self.cls: Optional[type] = None
        self.attrs: Dict[str, AttrEntry] = {}
        self.config: MoldConfig = MoldConfig() if config is None else config
        if o is not None:
            if isinstance(o, list):
                for ae in map(AttrEntry.ensure_it, o):
                    self.add_entry(ae)
            elif isinstance(o, dict):
                self.add_hints(o)
            else:
                self.add_hints(get_attr_hints(o))
                if isinstance(o, type):
                    self.cls = o
                    if hasattr(self.cls, "__mold_config__"):
                        self.config = self.cls.__mold_config__
                    self.set_defaults(self.get_defaults_from_cls(self.cls))
                    docstring = o.__doc__
                    dst = DocStringTemplate(docstring, {ATTRIBUTES})
                    self.syncup_dst_and_attrs(dst, ATTRIBUTES)
                    self.cls.__doc__ = dst.doc()

    def syncup_dst_and_attrs(self, dst: DocStringTemplate, section_name: str) -> None:
        groups = dst.var_groups
        if section_name not in groups:
            groups[section_name] = GroupOfVariables.empty(section_name)
        else:
            attr_docs = groups[section_name]
            for k in attr_docs.keys():
                self.attrs[k]._doc = str(attr_docs[k].content)
        variables = groups[section_name].variables
        for k in self.keys:
            if k not in variables:
                variables[k] = VariableDocEntry.empty(k)
            content = variables[k].content
            ae = self.attrs[k]
            content.insert(str(ae.typing))
            if ae.default is not None:
                content.end_of_sentence()
                content.append(f"Default is: {ae.default!r}.")

    @classmethod
    def __factory__(cls):
        return delegate_factory(cls, ("__mold__", "mold"))

    def add_hints(self, hints):
        for var_name, var_cls in hints.items():
            self.add_entry(AttrEntry(var_name, var_cls))

    def set_defaults(self, defaults):
        for k in self.attrs:
            if k in defaults:
                def_v = defaults[k]
                if def_v is not None:
                    self.attrs[k].default = def_v

    def get_defaults_from_cls(self, cls):
        return {
            attr_name: getattr(cls, attr_name)
            for attr_name in self.attrs
            if hasattr(cls, attr_name)
        }

    def get_defaults_from_fn(self, fn):
        names, _, _, defaults = getfullargspec(fn)[:4]
        if defaults is None:
            defaults = []
        def_offset = len(names) - len(defaults)
        return {k: v for k, v in zip(names[def_offset:], defaults) if k in self.attrs}

    def add_entry(self, entry: AttrEntry):
        if entry.index is not None:
            raise AssertionError(f"Same entry reused: {entry}")
        entry.index = len(self.attrs)
        self.keys.append(entry.name)
        self.attrs[entry.name] = entry

    def __to_json__(self):
        return [str(ae) for ae in self.attrs.values()]

    def check_overlaps(self, values):
        missing, not_known = self.find_overlaps(values)
        if len(missing) > 0:
            raise AttributeError(f"Required : {missing}")
        if len(not_known) > 0:
            raise AttributeError(f"Not known: {not_known}")

    def find_overlaps(self, values):
        missing = (
            set(ae.name for ae in self.attrs.values() if ae.required()) - values.keys()
        )
        not_known = set(values.keys()) - set(self.attrs.keys())
        return missing, not_known

    def build_val_dict(self, json_values):
        self.check_overlaps(json_values)
        return self.mold_dict(json_values, Conversion.TO_OBJECT)

    def mold_dict(
        self,
        in_data: Union[Tuple[Any, ...], List[Any], Dict[str, Any], DictLike],
        direction: Conversion,
        flator: Optional[Flator] = None,
    ) -> Dict[str, Any]:
        if isinstance(in_data, (tuple, list)):
            self.assert_row(in_data)
            in_data = dict(zip(self.keys, in_data))
        out_data = {}
        for k in self.keys:
            if k in in_data:
                v = self.attrs[k].convert(in_data[k], direction, flator)
            else:
                v = self.attrs[k].convert(None, direction, flator)
            if (
                v is not None
                or not (self.config.omit_optional_null)
                or direction.to_object()
                or not (self.attrs[k].is_optional())
            ):
                out_data[k] = v
        return out_data

    def dict_to_row(self, dct: Union[Dict[str, Any], DictLike]) -> Tuple[Any, ...]:
        return tuple(dct[k] if k in dct else None for k in self.keys)

    def mold_row(
        self,
        in_data: Sequence[Any],
        direction: Conversion,
        flator: Optional[Flator] = None,
    ) -> Tuple[Any, ...]:
        self.assert_row(in_data)
        return tuple(
            self.attrs[self.keys[i]].convert(in_data[i], direction, flator)
            for i in range(len(self.keys))
        )

    def assert_row(self, in_data):
        if len(self.keys) != len(in_data):
            raise AttributeError(f"arrays has to match in size: {self.keys} {in_data}")

    def set_attrs(self, values, target):
        for k, v in self.build_val_dict(values).items():
            setattr(target, k, v)

    def pull_attrs(self, from_obj: Any) -> Dict[str, Any]:
        """
        extract known attributes into dictionary
        """
        return {k: getattr(from_obj, k) for k in self.keys if hasattr(from_obj, k)}

    def deflate(self, v:Dict[str, Any], flator:Flator ):
        return self.mold_dict(v, Conversion.DEFLATE, flator)

    def inflate(self, v:Dict[str, Any], flator:Flator ):
        return self.mold_dict(v, Conversion.INFLATE, flator)

    def wrap_result(self, v):
        if self.is_single_return():
            return {SINGLE_RETURN_VALUE: v}
        elif isinstance(v, tuple):
            return dict(zip(self.keys, v))
        else:
            dict_like = DictLike(v)
            return { k:dict_like[k] for k in self.keys}

    def is_single_return(self) -> bool:
        return self.keys == [SINGLE_RETURN_VALUE]

    def is_empty(self) -> bool:
        return len(self.keys) == 0

class FunctionMold:
    in_mold: Mold
    out_mold: Mold
    dst: DocStringTemplate

    def __init__(self, fn: Callable[..., Any]):

        """
        Args:
            fn: function inspected

        Returns:
            in_mold: `Mold` of function input
            out_mold: `Mold` of function output
            dst: template

        >>> def a(i:int)->None:
        ...     pass
        ...
        >>> amold = FunctionMold(a)
        >>> amold.out_mold.is_empty()
        True
        >>> amold.out_mold.is_single_return()
        False
        >>>
        >>> def b(i:int)->str:
        ...     return f'i={i}'
        ...
        >>> bmold = FunctionMold(b)
        >>> bmold.out_mold.is_empty()
        False
        >>> bmold.out_mold.is_single_return()
        True
        >>> def c(i:int)->Optional[Tuple[str,int]]:
        ...     return f'i={i}', i
        ...
        >>> cmold = FunctionMold(c)
        >>> print(cmold.dst.doc()) #doctest: +NORMALIZE_WHITESPACE
            Args:
                i: Required[int]
            Returns:
                v0: Required[str]
                v1: Required[int]
        >>>
        """
        self.fn = fn
        self.dst = DocStringTemplate(fn.__doc__, {ARGS, RETURNS})

        annotations = dict(get_attr_hints(fn))
        return_type = annotations["return"]
        del annotations["return"]
        self.in_mold = Mold(annotations)
        self.in_mold.set_defaults(self.in_mold.get_defaults_from_fn(fn))
        self.out_mold = Mold()

        if return_type != type(None):
            optional = is_optional(return_type)
            if optional:
                return_type = get_args(return_type)[0]
            if is_tuple(return_type):
                args = get_args(return_type)
                keys = [f"v{i}" for i in range(len(args))]
                if RETURNS in self.dst.var_groups:
                    for i, k in enumerate(self.dst.var_groups[RETURNS].keys()):
                        keys[i] = k
                self.out_mold.add_hints(dict(zip(keys, args)))
            else:
                out_hints = get_attr_hints(return_type)
                if not (is_primitive(return_type)) and len(out_hints) > 0:
                    self.out_mold.add_hints(out_hints)
                else:
                    ae = AttrEntry(SINGLE_RETURN_VALUE, return_type)
                    self.out_mold.add_entry(ae)
        self.in_mold.syncup_dst_and_attrs(self.dst, ARGS)
        self.out_mold.syncup_dst_and_attrs(self.dst, RETURNS)

    def __call__(self, kwargs):
        return self.out_mold.wrap_result(self.fn(**kwargs))




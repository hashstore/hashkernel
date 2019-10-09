from inspect import getfullargspec
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from hashkernel.packer import SIZED_BYTES, UTF8_STR, Packer, ProxyPacker, TuplePacker

from . import (
    ClassRef,
    Conversion,
    DictLike,
    EnsureIt,
    GlobalRef,
    Jsonable,
    Stringable,
    delegate_factory,
    is_primitive,
    json_decode,
    json_encode,
    reraise_with_msg,
    to_dict,
    to_json,
    to_tuple,
    utf8_decode,
)
from .docs import DocStringTemplate, GroupOfVariables, VariableDocEntry
from .typings import (
    OnlyAnnotatedProperties,
    get_args,
    get_attr_hints,
    is_dict,
    is_list,
    is_optional,
    is_tuple,
)

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

    def convert(self, v: Any, direction: Conversion) -> Any:
        return self.val_cref.convert(v, direction)

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

    def convert(self, in_v: Any, direction: Conversion) -> Dict[Any, Any]:
        return {
            self.key_cref.convert(k, direction): self.val_cref.convert(v, direction)
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

    def convert(self, in_v: Any, direction: Conversion) -> List[Any]:
        return [self.val_cref.convert(v, direction) for v in in_v]

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

    def convert(self, v: Any, direction: Conversion) -> Any:
        try:
            if Conversion.TO_OBJECT == direction:
                if v is None:
                    if self.default is not None:
                        return self.default
                    else:
                        return self.typing.default()
                else:
                    return self.typing.convert(v, Conversion.TO_OBJECT)
            else:
                if v is None:
                    return None
                else:
                    return self.typing.convert(v, Conversion.TO_JSON)
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

    def __init__(self, o=None):
        self.keys: List[str] = []
        self.cls: Optional[type] = None
        self.attrs: Dict[str, AttrEntry] = {}
        self.config: MoldConfig = MoldConfig()
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
        missing = (
            set(ae.name for ae in self.attrs.values() if ae.required()) - values.keys()
        )
        if len(missing) > 0:
            raise AttributeError(f"Required : {missing}")
        not_known = set(values.keys()) - set(self.attrs.keys())
        if len(not_known) > 0:
            raise AttributeError(f"Not known: {not_known}")

    def build_val_dict(self, json_values):
        self.check_overlaps(json_values)
        return self.mold_dict(json_values, Conversion.TO_OBJECT)

    def mold_dict(
        self,
        in_data: Union[Tuple[Any, ...], List[Any], Dict[str, Any], DictLike],
        direction: Conversion,
    ) -> Dict[str, Any]:
        if isinstance(in_data, (tuple, list)):
            self.assert_row(in_data)
            in_data = dict(zip(self.keys, in_data))
        out_data = {}
        for k in self.keys:
            if k in in_data:
                v = self.attrs[k].convert(in_data[k], direction)
            else:
                v = self.attrs[k].convert(None, direction)
            if (
                v is not None
                or not (self.config.omit_optional_null)
                or direction != Conversion.TO_JSON
                or not (self.attrs[k].is_optional())
            ):
                out_data[k] = v
        return out_data

    def dict_to_row(self, dct: Union[Dict[str, Any], DictLike]) -> Tuple[Any, ...]:
        return tuple(dct[k] if k in dct else None for k in self.keys)

    def mold_row(
        self, in_data: Sequence[Any], direction: Conversion
    ) -> Tuple[Any, ...]:
        self.assert_row(in_data)
        return tuple(
            self.attrs[self.keys[i]].convert(in_data[i], direction)
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

    def wrap_input(self, v):
        v_dct = self.mold_dict(v, Conversion.TO_OBJECT)
        if self.cls is not None:
            return self.cls(v_dct)
        return v_dct

    def output_json(self, v):
        if self.is_single_return():
            result = {SINGLE_RETURN_VALUE: v}
        else:
            result = DictLike(v)
        return self.mold_dict(result, Conversion.TO_JSON)

    def is_single_return(self) -> bool:
        return self.keys == [SINGLE_RETURN_VALUE]

    def is_empty(self) -> bool:
        return len(self.keys) == 0


def extract_molds_from_function(
    fn: Callable[..., Any]
) -> Tuple[Mold, Mold, DocStringTemplate]:
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
    >>> in_a, out_a,_ = extract_molds_from_function(a)
    >>> out_a.is_empty()
    True
    >>> out_a.is_single_return()
    False
    >>>
    >>> def b(i:int)->str:
    ...     return f'i={i}'
    ...
    >>> in_b, out_b,_ = extract_molds_from_function(b)
    >>> out_b.is_empty()
    False
    >>> out_b.is_single_return()
    True
    >>> def c(i:int)->Optional[Tuple[str,int]]:
    ...     return f'i={i}', i
    ...
    >>> in_b, out_b, dst = extract_molds_from_function(c)
    >>> print(dst.doc()) #doctest: +NORMALIZE_WHITESPACE
        Args:
            i: Required[int]
        Returns:
            v0: Required[str]
            v1: Required[int]
    >>>
    """
    dst = DocStringTemplate(fn.__doc__, {ARGS, RETURNS})

    annotations = dict(get_attr_hints(fn))
    return_type = annotations["return"]
    del annotations["return"]
    in_mold = Mold(annotations)
    in_mold.set_defaults(in_mold.get_defaults_from_fn(fn))
    out_mold = Mold()

    if return_type != type(None):
        optional = is_optional(return_type)
        if optional:
            return_type = get_args(return_type)[0]
        if is_tuple(return_type):
            args = get_args(return_type)
            keys = [f"v{i}" for i in range(len(args))]
            if RETURNS in dst.var_groups:
                for i, k in enumerate(dst.var_groups[RETURNS].keys()):
                    keys[i] = k
            out_mold.add_hints(dict(zip(keys, args)))
        else:
            out_hints = get_attr_hints(return_type)
            if not (is_primitive(return_type)) and len(out_hints) > 0:
                out_mold.add_hints(out_hints)
            else:
                ae = AttrEntry(SINGLE_RETURN_VALUE, return_type)
                out_mold.add_entry(ae)
    in_mold.syncup_dst_and_attrs(dst, ARGS)
    out_mold.syncup_dst_and_attrs(dst, RETURNS)
    return in_mold, out_mold, dst


class _AnnotationsProcessor(type):
    def __init__(cls, name, bases, dct):
        cls.__mold__ = Mold(cls)
        if hasattr(cls, "__attribute_packers__"):
            cls.__packer__ = ProxyPacker(
                cls, TuplePacker(*cls.__attribute_packers__), to_tuple, cls
            )
        else:
            cls.__packer__ = ProxyPacker(cls, UTF8_STR, str, cls)
        if hasattr(cls, "__serialize_as__"):
            cls.__serialization_mold__: Mold = Mold.ensure_it(
                cls.__serialize_as__
            )  # type: ignore
        else:
            cls.__serialization_mold__ = cls.__mold__


def combine_vars(
    vars: Optional[Dict[str, Any]], kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    if vars is None:
        vars = {}
    vars.update(kwargs)
    return vars


class SmAttr(Jsonable, metaclass=_AnnotationsProcessor):
    """
    Mixin - supports annotations:
      a:X
      a:List[X]
      a:Dict[K,V]
      a:Optional[X]
      x:datetime
      x:date

    >>> from datetime import date, datetime
    >>> from hashkernel.tests import StringableExample
    >>> class A(SmAttr):
    ...     x:int
    ...     z:bool
    ...
    >>> A.__mold__.attrs #doctest: +NORMALIZE_WHITESPACE
    {'x': AttrEntry('x:Required[int]'),
    'z': AttrEntry('z:Required[bool]')}
    >>> A({"x":3})
    Traceback (most recent call last):
    ...
    AttributeError: Required : {'z'}
    >>> A({"x":3, "z":False, "q":"asdf"})
    Traceback (most recent call last):
    ...
    AttributeError: Not known: {'q'}
    >>> a = A({"x":747, "z":False})
    >>> str(a)
    '{"x": 747, "z": false}'
    >>> class A2(SmAttr):
    ...     x:int
    ...     z:Optional[date]
    ...
    >>> A2.__mold__.attrs #doctest: +NORMALIZE_WHITESPACE
    {'x': AttrEntry('x:Required[int]'),
    'z': AttrEntry('z:Optional[datetime:date]')}
    >>> class B(SmAttr):
    ...     x: StringableExample
    ...     aa: List[A2]
    ...     dt: Dict[datetime, A]
    ...
    >>> B.__mold__.attrs #doctest: +NORMALIZE_WHITESPACE
    {'x': AttrEntry('x:Required[hashkernel.tests:StringableExample]'),
    'aa': AttrEntry('aa:List[hashkernel.smattr:A2]'),
    'dt': AttrEntry('dt:Dict[datetime:datetime,hashkernel.smattr:A]')}
    >>> b = B({"x":"3X8X3D7svYk0rD1ncTDRTnJ81538A6ZdSPcJVsptDNYt" })
    >>> str(b) #doctest: +NORMALIZE_WHITESPACE
    '{"aa": [], "dt": {}, "x": "3X8X3D7svYk0rD1ncTDRTnJ81538A6ZdSPcJVsptDNYt"}'
    >>> b = B({"x":"3X8X3D7svYk0rD1ncTDRTnJ81538A6ZdSPcJVsptDNYt", "aa":[{"x":5,"z":"2018-06-30"},{"x":3}] })
    >>> str(b) #doctest: +NORMALIZE_WHITESPACE
    '{"aa": [{"x": 5, "z": "2018-06-30"}, {"x": 3, "z": null}], "dt": {},
      "x": "3X8X3D7svYk0rD1ncTDRTnJ81538A6ZdSPcJVsptDNYt"}'
    >>> a2 = A2({"x":747, "z":date(2018,6,30)})
    >>> str(a2)
    '{"x": 747, "z": "2018-06-30"}'
    >>> a2m = A2({"x":777}) #dict input
    >>> str(a2m)
    '{"x": 777, "z": null}'
    >>> class A2z(SmAttr):
    ...     __mold_config__ = MoldConfig(omit_optional_null = True)
    ...     x:int
    ...     z:Optional[date]
    ...
    >>> str(A2z({"x":777})) #null should be omited
    '{"x": 777}'
    >>> str(A2(x=777)) #kwargs input
    '{"x": 777, "z": null}'
    >>> A2()
    Traceback (most recent call last):
    ...
    AttributeError: Required : {'x'}
    >>> b=B({"x":StringableExample("3X8X3D7svYk0rD1ncTDRTnJ81538A6ZdSPcJVsptDNYt"),
    ...     "aa":[a2m,{"x":3}],
    ...     'dt':{datetime(2018,6,30,16,18,27,267515) :a}})
    ...
    >>> str(b) #doctest: +NORMALIZE_WHITESPACE
    '{"aa": [{"x": 777, "z": null}, {"x": 3, "z": null}],
    "dt": {"2018-06-30T16:18:27.267515": {"x": 747, "z": false}},
    "x": "3X8X3D7svYk0rD1ncTDRTnJ81538A6ZdSPcJVsptDNYt"}'
    >>> str(B(to_json(b))) #doctest: +NORMALIZE_WHITESPACE
    '{"aa": [{"x": 777, "z": null}, {"x": 3, "z": null}],
    "dt": {"2018-06-30T16:18:27.267515": {"x": 747, "z": false}},
    "x": "3X8X3D7svYk0rD1ncTDRTnJ81538A6ZdSPcJVsptDNYt"}'
    >>> str(B(bytes(b))) #doctest: +NORMALIZE_WHITESPACE
    '{"aa": [{"x": 777, "z": null}, {"x": 3, "z": null}],
    "dt": {"2018-06-30T16:18:27.267515": {"x": 747, "z": false}},
    "x": "3X8X3D7svYk0rD1ncTDRTnJ81538A6ZdSPcJVsptDNYt"}'
    >>> str(B(to_tuple(b))) #doctest: +NORMALIZE_WHITESPACE
    '{"aa": [{"x": 777, "z": null}, {"x": 3, "z": null}],
    "dt": {"2018-06-30T16:18:27.267515": {"x": 747, "z": false}},
    "x": "3X8X3D7svYk0rD1ncTDRTnJ81538A6ZdSPcJVsptDNYt"}'
    >>> str(B(to_dict(b))) #doctest: +NORMALIZE_WHITESPACE
    '{"aa": [{"x": 777, "z": null}, {"x": 3, "z": null}],
    "dt": {"2018-06-30T16:18:27.267515": {"x": 747, "z": false}},
    "x": "3X8X3D7svYk0rD1ncTDRTnJ81538A6ZdSPcJVsptDNYt"}'
    >>> class O(SmAttr):
    ...     x:int
    ...     z:bool = False
    ...
    >>> str(O({"x":5}))
    '{"x": 5, "z": false}'
    >>> str(O({"x":5, "z": True}))
    '{"x": 5, "z": true}'
    >>> class P(O):
    ...    a: float
    ...
    >>> str(P({'x':5,'a':1.03e-5}))
    '{"a": 1.03e-05, "x": 5, "z": false}'
    """

    __mold_config__: ClassVar[MoldConfig]
    __mold__: ClassVar[Mold]
    __serialization_mold__: ClassVar[Mold]
    __packer__: ClassVar[Packer]

    def __init__(
        self,
        _vals_: Union[None, bytes, str, Iterable[Any], Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        mold = self.__mold__

        if isinstance(_vals_, bytes):
            _vals_ = utf8_decode(_vals_)
        vals_dict: Dict[str, Any] = {}
        if _vals_ is None:
            pass
        elif isinstance(_vals_, str):
            vals_dict = json_decode(_vals_)
        elif isinstance(_vals_, dict):
            vals_dict = _vals_
        elif hasattr(_vals_, "__iter__"):
            vals_dict = mold.mold_dict(list(_vals_), Conversion.TO_OBJECT)
        else:
            raise AssertionError(f"cannot construct from: {_vals_}")
        vals_dict.update(kwargs)
        values = {k: v for k, v in vals_dict.items() if v is not None}
        mold.set_attrs(values, self)
        if hasattr(self, "__validate__"):
            self.__validate__()  # type: ignore

    def __to_json__(self) -> Dict[str, Any]:
        return self.__serialization_mold__.mold_dict(DictLike(self), Conversion.TO_JSON)

    def __to_dict__(self) -> Dict[str, Any]:
        return self.__serialization_mold__.pull_attrs(self)

    def __to_tuple__(self) -> tuple:
        return tuple(getattr(self, k) for k in self.__serialization_mold__.keys)


class JsonWrap(SmAttr):
    classRef: GlobalRef
    json: Optional[Any]

    def unwrap(self):
        return self.classRef.get_instance()(self.json)

    @classmethod
    def wrap(cls, o):
        if isinstance(o, Jsonable):
            return cls({"classRef": GlobalRef(type(o)), "json": to_json(o)})
        raise AttributeError(f"Not jsonable: {o}")


class BytesWrap(SmAttr):

    __attribute_packers__ = (
        ProxyPacker(GlobalRef, UTF8_STR, str, GlobalRef),
        SIZED_BYTES,
    )

    classRef: GlobalRef
    content: bytes

    def unwrap(self):
        return self.classRef.get_instance()(self.content)

    @classmethod
    def wrap(cls, o):
        return cls(classRef=GlobalRef(type(o)), content=bytes(o))

    def __bytes__(self):
        return self.__packer__.pack(self)

    def ___factory__(cls):
        def factory(input):
            if isinstance(input, bytes):
                return cls.__packer__.pack(input)
            return cls(input)

        return factory


def build_named_tuple_packer(
    cls: type, mapper: Callable[[type], Packer]
) -> TuplePacker:
    mold = Mold(cls)
    comp_classes = (a.typing.val_cref.cls for a in mold.attrs.values())
    return TuplePacker(*map(mapper, comp_classes), cls=cls)

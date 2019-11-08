from typing import Any, ClassVar, Dict, Iterable, List, Optional, Union

from hashkernel.packer import (
    SIZED_BYTES,
    UTF8_STR,
    Packer,
    PackerFactory,
    ProxyPacker,
    TuplePacker,
)

from . import (
    DictLike,
    GlobalRef,
    Jsonable,
    json_decode,
    to_dict,
    to_json,
    to_tuple,
    utf8_decode,
)
from .mold import Conversion, Mold, MoldConfig


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
            raise AssertionError(f"cannot construct from: {_vals_!r}")
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


def build_named_tuple_packer(cls: type, mapper: PackerFactory) -> TuplePacker:
    mold = Mold(cls)
    comp_classes = (a.typing.val_cref.cls for a in mold.attrs.values())
    return TuplePacker(*map(mapper, comp_classes), cls=cls)

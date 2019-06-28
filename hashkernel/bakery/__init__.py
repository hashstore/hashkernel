#!/usr/bin/env python
# -*- coding: utf-8 -*-

import enum
import logging
from typing import Any, List, Optional

from hashkernel import ClassRef, CodeEnum, GlobalRef
from hashkernel.smattr import BytesWrap, JsonWrap, SmAttr

log = logging.getLogger(__name__)

Cake_REF, CakeRack_REF, QuestionMsg_REF, DataChunkMsg_REF, ResponseMsg_REF = (
    GlobalRef(f"hashkernel.bakery.cake:{n}")
    for n in ("Cake", "CakeRack", "QuestionMsg", "DataChunkMsg", "ResponseMsg")
)


class CakeProperties(enum.Enum):
    IS_INLINED = enum.auto()
    IS_IMMUTABLE = enum.auto()
    IS_RESOLVED = enum.auto()
    IS_FOLDER = enum.auto()
    IS_GUID = enum.auto()
    IS_JOURNAL = enum.auto()
    IS_VTREE = enum.auto()

    def __str__(self) -> str:
        return self.name.lower()

    @staticmethod
    def set_properties(target: Any, *modifiers: "CakeProperties") -> None:
        for e in CakeProperties:
            setattr(target, str(e), e in modifiers)

    @staticmethod
    def typings() -> None:
        for e in CakeProperties:
            print(f"{e}:bool")


class CakeMode(CodeEnum):
    INLINE = (0, CakeProperties.IS_IMMUTABLE, CakeProperties.IS_INLINED)
    SHA256 = (1, CakeProperties.IS_IMMUTABLE, CakeProperties.IS_RESOLVED)
    GUID = (2, CakeProperties.IS_GUID)

    def __init__(self, code: int, *modifiers: CakeProperties) -> None:
        CodeEnum.__init__(self, code)
        self.modifiers = modifiers


class CakeType(SmAttr):
    mode: Optional[CakeMode] = None
    modifiers: List[CakeProperties]
    gref: Optional[GlobalRef]

    def cref(self) -> Optional[ClassRef]:
        return ClassRef.ensure_it_or_none(self.gref)

    def __str__(self):
        return f"mode={self.mode},gref={self.gref}," f"modifiers={self.modifiers}"

    def __repr__(self):
        return f"{type(self).__name__}({str(self)})"


class TypesProcessor(type):
    def __init__(cls, name, bases, dct):
        x = []
        for k in dct:
            if k[:1] != "_" and isinstance(dct[k], CakeType):
                ct: CakeType = dct[k]
                if ct.mode is None:
                    x.append((f"{k}_INLINE", CakeMode.INLINE, ct))
                    x.append((f"{k}", CakeMode.SHA256, ct))
                elif ct.mode == CakeMode.GUID:
                    x.append((k, ct.mode, ct))
                else:
                    raise AssertionError(
                        "mode should be GUID or None. None means "
                        "immutable data and expanded to INLINE and SHA"
                    )
        cls.__headers__ = x
        cls.__start_index__ = getattr(cls, "__start_index__", 0)


class CakeTypes(metaclass=TypesProcessor):
    """
    >>> issubclass(type(CakeTypes), TypesProcessor)
    True
    """

    NO_CLASS = CakeType()
    JOURNAL = CakeType(mode=CakeMode.GUID, modifiers=[CakeProperties.IS_JOURNAL])
    FOLDER = CakeType(modifiers=[CakeProperties.IS_FOLDER], gref=CakeRack_REF)
    TIMESTAMP = CakeType(mode=CakeMode.GUID)
    QUESTION_MSG = CakeType(gref=QuestionMsg_REF)
    RESPONSE_MSG = CakeType(gref=ResponseMsg_REF)
    DATA_CHUNK_MSG = CakeType(gref=DataChunkMsg_REF)
    JSON_WRAP = CakeType(gref=GlobalRef(JsonWrap))
    BYTES_WRAP = CakeType(gref=GlobalRef(BytesWrap))
    JOURNAL_FOLDER = CakeType(
        mode=CakeMode.GUID,
        gref=CakeRack_REF,
        modifiers=[CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL],
    )
    VTREE_FOLDER = CakeType(
        mode=CakeMode.GUID,
        modifiers=[CakeProperties.IS_FOLDER, CakeProperties.IS_VTREE],
    )
    MOUNT_FOLDER = CakeType(
        mode=CakeMode.GUID,
        modifiers=[CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL],
    )
    SESSION = CakeType(mode=CakeMode.GUID)
    NODE = CakeType(
        mode=CakeMode.GUID,
        modifiers=[CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL],
    )
    USER = CakeType(
        mode=CakeMode.GUID,
        modifiers=[CakeProperties.IS_FOLDER, CakeProperties.IS_JOURNAL],
    )

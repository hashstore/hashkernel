#!/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Any, Callable, Dict, Optional, Union

from hashkernel import Jsonable, json_decode, utf8_decode
from hashkernel.bakery import Cake
from hashkernel.mold import Conversion, Mold
from hashkernel.packer import (
    GREEDY_BYTES,
    Packer,
    PackerDefinitions,
    PackerLibrary,
    ProxyPacker,
)
from hashkernel.plugins import query_plugins
from hashkernel.smattr import SmAttr

CAKEABLES = PackerLibrary()

PackerDefinitions((Jsonable, lambda t: ProxyPacker(t, GREEDY_BYTES))).register_all(
    CAKEABLES
)

for pdefs in query_plugins(PackerDefinitions, "hashkernel.bakery.cakables"):
    pdefs.register_all(CAKEABLES)


class Cakeable:
    packer: Packer
    cake: Optional[Cake]
    data: Any

    def __init__(self, packer: Packer, cake: Optional[Cake], data: Any):
        self.packer = packer
        assert cake is not None or data is not None
        self.cake = cake
        self.data = data

    @classmethod
    def from_data(cls, packer: Packer, data: Any):
        return cls(packer, None, data)

    @classmethod
    def from_cake(cls, packer: Packer, cake: Cake):
        return cls(packer, cake, None)

    def __to_json__(self):
        assert self.cake is not None
        return self.cake

    def load(self, loader: Callable[[Cake], bytes]):
        assert self.need_to_be_loaded()
        buffer = loader(self.cake)
        self.data = self.packer.unpack_whole_buffer(buffer)

    def need_to_be_loaded(self):
        return self.data is None and self.cake is not None

    def store(self, store_fn: Callable[[bytes], Cake]):
        assert self.need_to_be_stored()
        buffer = self.packer.pack(self.data)
        self.cake = store_fn(buffer)

    def need_to_be_stored(self):
        return self.data is not None and self.cake is None


class Signal:
    """
    Signal - Transput (input or output). Data consumed or produced
    by task
    """

    mold: Mold
    store: Dict[str, Any]

    def __init__(
        self,
        mold: Mold,
        s: Union[Dict[str, Any], str, bytes, None] = None,
        pack_lib: PackerLibrary = CAKEABLES,
    ):
        store: Dict[str, Any] = {}
        if s is not None:
            if isinstance(s, dict):
                store.update(s)
            elif isinstance(s, str):
                store.update(json_decode(s))
            elif isinstance(s, bytes):
                store.update(json_decode(utf8_decode(s)))
            else:
                raise AssertionError(f"Unexpected object: {s!r}")
        mold.check_overlaps(store)
        for ae in mold.attrs.values():
            if ae.name in store:
                v = ae.convert(store[ae.name], Conversion.TO_OBJECT)
                packer = pack_lib[ae.typing.val_cref.cls]
                if packer is not None:
                    v = Cakeable.from_data(packer, v)
                store[ae.name] = v
        self.mold = mold
        self.store = store


class CodeBase(SmAttr):
    pass


class Question(SmAttr):
    method: str
    input: Signal


class Answer(SmAttr):
    question: Cake
    output: Signal


class Invocation(SmAttr):
    recipe: Question
    codebase: CodeBase
    result: Answer

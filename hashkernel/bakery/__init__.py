#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import logging
import threading
from contextlib import contextmanager
from functools import total_ordering
from typing import IO, Any, Callable, Dict, Iterable, List, Optional, Union

from hashkernel import GlobalRef
from hashkernel.ake import NULL_CAKE, SIZEOF_CAKE, Cake, HasCakeFromBytes
from hashkernel.hashing import BytesOrderingMixin
from hashkernel.smattr import SmAttr

log = logging.getLogger(__name__)


class QuestionMsg(SmAttr, HasCakeFromBytes):
    ref: GlobalRef
    data: Dict[str, Any]


class ResponseChain(SmAttr, HasCakeFromBytes):
    previous: Cake


class DataChunkMsg(ResponseChain, HasCakeFromBytes):
    data: Any


class ResponseMsg(ResponseChain, HasCakeFromBytes):
    data: Dict[str, Any]
    traceback: Optional[str] = None

    def is_error(self):
        return self.traceback is not None


@total_ordering
class BlockStream(BytesOrderingMixin):
    """
    >>> bs = BlockStream(blocks=[NULL_CAKE, NULL_CAKE])
    >>> len(bytes(bs))
    64
    >>> bs == BlockStream(bytes(bs))
    True
    >>> bs != BlockStream(bytes(bs))
    False
    """

    blocks: List[Cake]

    def __init__(
        self, buffer: Optional[bytes] = None, blocks: Optional[Iterable[Cake]] = None
    ):
        if buffer is not None:
            assert blocks is None
            len_of = len(buffer)
            assert len_of % SIZEOF_CAKE == 0
            self.blocks = []
            offset = 0
            for _ in range(len_of // SIZEOF_CAKE):
                end = offset + SIZEOF_CAKE
                self.blocks.append(Cake(buffer[offset:end]))
                offset = end
        else:
            assert blocks is not None
            self.blocks = list(blocks)

    def __bytes__(self):
        return b"".join(map(bytes, self.blocks))


class HashSession(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def load_content(self, cake: Cake) -> bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    async def store_content(self, content: Union[bytes, IO[bytes]]) -> Cake:
        raise NotImplementedError("subclasses must override")

    def close(self):
        pass


class HashContext:
    @staticmethod
    def get() -> HashSession:
        return threading.local().hash_ctx

    @staticmethod
    def set(ctx: HashSession):
        if ctx is None:
            try:
                del threading.local().hash_ctx
            except AttributeError:
                pass
        else:
            (threading.local()).hash_ctx = ctx

    @staticmethod
    @contextmanager
    def context(factory: Callable[[], HashSession]):
        session = factory()
        HashContext.set(session)
        try:
            yield session
        finally:
            HashContext.set(None)
            session.close()

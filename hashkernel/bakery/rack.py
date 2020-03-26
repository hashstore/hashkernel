import json
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from hashkernel import Jsonable, utf8_decode, utf8_encode
from hashkernel.hashing import Hasher, HashKey


class HashRack(Jsonable):
    """
    sorted dictionary of names and corresponding HashKeys

    >>> short_k = HashKey.from_bytes(b'The quick brown fox jumps over')
    >>> longer_k = HashKey.from_bytes(b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.')

    >>> cakes = HashRack()
    >>> cakes['short'] = short_k
    >>> cakes['longer'] = longer_k
    >>> len(cakes)
    2

    >>> cakes.keys()
    ['longer', 'short']
    >>> str(cakes.cake())
    '8z2uTCVRzKr51iplxCCAtuPLE06jfIN5J802HPp73SP'
    >>> cakes.size()
    117
    >>> cakes.content()
    '[["longer", "short"], ["zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT", "l01natqrQGg1ueJkFIc9mUYt18gcJjdsPLSLyzGgjY7"]]'
    >>> cakes.get_name_by_cake("zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT")
    'longer'
    """

    def __init__(self, o: Any = None) -> None:
        self.store: Dict[str, Optional[HashKey]] = {}
        self._clear_cached()
        if o is not None:
            self.parse(o)

    def _clear_cached(self):
        self._inverse: Any = None
        self._cake: Any = None
        self._content: Any = None
        self._size: Any = None
        self._in_bytes: Any = None
        self._defined: Any = None

    def inverse(self) -> Dict[HashKey, str]:
        if self._inverse is None:
            self._inverse = {v: k for k, v in self.store.items() if v is not None}
        return self._inverse

    def cake(self) -> HashKey:
        if self._cake is None:
            self._cake = HashKey(Hasher().update(bytes(self)))
        return self._cake

    def content(self) -> str:
        if self._content is None:
            self._content = str(self)
        return self._content

    def __bytes__(self) -> bytes:
        if self._in_bytes is None:
            self._in_bytes = utf8_encode(self.content())
        return self._in_bytes

    def size(self) -> int:
        if self._size is None:
            self._size = len(bytes(self))
        return self._size

    def is_defined(self) -> bool:
        if self._defined is None:
            self._defined = all(v is not None for v in self.store.values())
        return self._defined

    def parse(self, o: Any) -> "HashRack":
        self._clear_cached()
        if isinstance(o, bytes):
            names, cakes = json.loads(utf8_decode(o))
        elif isinstance(o, str):
            names, cakes = json.loads(o)
        elif type(o) in [list, tuple] and len(o) == 2:
            names, cakes = o
        else:
            names, cakes = json.load(o)
        self.store.update(zip(names, map(HashKey.ensure_it_or_none, cakes)))
        return self

    def __iter__(self) -> Iterable[str]:
        return iter(self.keys())

    def __setitem__(self, k: str, v: Union[HashKey, str, None]) -> None:
        self._clear_cached()
        self.store[k] = HashKey.ensure_it_or_none(v)

    def __delitem__(self, k: str):
        self._clear_cached()
        del self.store[k]

    def __getitem__(self, k: str) -> Optional[HashKey]:
        return self.store[k]

    def __len__(self) -> int:
        return len(self.store)

    def __contains__(self, k: str) -> bool:
        return k in self.store

    def get_name_by_cake(self, k: Union[HashKey, str]):
        return self.inverse()[HashKey.ensure_it(k)]

    def keys(self) -> List[str]:
        names = list(self.store.keys())
        names.sort()
        return names

    def get_cakes(self, names=None) -> List[Optional[HashKey]]:
        if names is None:
            names = self.keys()
        return [self.store[k] for k in names]

    def __to_json__(self) -> Tuple[List[str], List[Optional[HashKey]]]:
        keys = self.keys()
        return (keys, self.get_cakes(keys))



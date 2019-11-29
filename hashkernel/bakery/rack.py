import enum
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from hashkernel import Jsonable, utf8_decode, utf8_encode
from hashkernel.bakery import Cake, CakeTypes, HasCake
from hashkernel.hashing import Hasher
from hashkernel.smattr import SmAttr


class RackRow(SmAttr):
    name: str
    cake: Optional[Cake]


class CakeRack(Jsonable):
    """
    sorted dictionary of names and corresponding Cakes

    >>> short_k = Cake.from_bytes(b'The quick brown fox jumps over')
    >>> longer_k = Cake.from_bytes(b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.')

    >>> cakes = CakeRack()
    >>> cakes['short'] = short_k
    >>> cakes['longer'] = longer_k
    >>> len(cakes)
    2

    >>> cakes.keys()
    ['longer', 'short']
    >>> str(cakes.cake())
    'inKmqrDcAjuC8gutBPj2cZusI359bDzkl11frGBTF892'
    >>> cakes.size()
    119
    >>> cakes.content()
    '[["longer", "short"], ["zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT0", "l01natqrQGg1ueJkFIc9mUYt18gcJjdsPLSLyzGgjY70"]]'
    >>> cakes.get_name_by_cake("zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT0")
    'longer'
    """

    def __init__(self, o: Any = None) -> None:
        self.store: Dict[str, Optional[Cake]] = {}
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

    def inverse(self) -> Dict[Optional[Cake], str]:
        if self._inverse is None:
            self._inverse = {v: k for k, v in self.store.items()}
        return self._inverse

    def cake(self) -> Cake:
        if self._cake is None:
            self._cake = Cake(
                None,
                digest=Hasher().update(bytes(self)).digest(),
                type=CakeTypes.FOLDER,
            )
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

    def parse(self, o: Any) -> "CakeRack":
        self._clear_cached()
        if isinstance(o, bytes):
            names, cakes = json.loads(utf8_decode(o))
        elif isinstance(o, str):
            names, cakes = json.loads(o)
        elif type(o) in [list, tuple] and len(o) == 2:
            names, cakes = o
        else:
            names, cakes = json.load(o)
        self.store.update(zip(names, map(Cake.ensure_it_or_none, cakes)))
        return self

    def __iter__(self) -> Iterable[str]:
        return iter(self.keys())

    def __setitem__(self, k: str, v: Union[Cake, str, None]) -> None:
        self._clear_cached()
        self.store[k] = Cake.ensure_it_or_none(v)

    def __delitem__(self, k: str):
        self._clear_cached()
        del self.store[k]

    def __getitem__(self, k: str) -> Optional[Cake]:
        return self.store[k]

    def __len__(self) -> int:
        return len(self.store)

    def __contains__(self, k: str) -> bool:
        return k in self.store

    def get_name_by_cake(self, k: Union[Cake, str]):
        return self.inverse()[Cake.ensure_it(k)]

    def keys(self) -> List[str]:
        names = list(self.store.keys())
        names.sort()
        return names

    def get_cakes(self, names=None) -> List[Optional[Cake]]:
        if names is None:
            names = self.keys()
        return [self.store[k] for k in names]

    def __to_json__(self) -> Tuple[List[str], List[Optional[Cake]]]:
        keys = self.keys()
        return (keys, self.get_cakes(keys))


HasCake.register(CakeRack)

CakeTypes.FOLDER.update_gref(CakeRack)

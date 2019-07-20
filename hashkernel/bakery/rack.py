import enum
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from hashkernel import Jsonable, utf8_decode, utf8_encode
from hashkernel.bakery import Cake, CakeTypes, HasCake
from hashkernel.hashing import Hasher
from hashkernel.smattr import SmAttr


class PatchAction(Jsonable, enum.Enum):
    update = +1
    delete = -1

    @classmethod
    def __factory__(cls):
        return lambda s: cls[s]

    def __str__(self):
        return self.name

    def __to_json__(self):
        return str(self)


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
                None, digest=Hasher(bytes(self)).digest(), type=CakeTypes.FOLDER
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

    def merge(
        self, previous: "CakeRack"
    ) -> Iterable[Tuple[PatchAction, str, Optional[Cake]]]:
        """
        >>> o1 = Cake.from_bytes(b'The quick brown fox jumps over')
        >>> o2v1 = Cake.from_bytes(b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.')
        >>> o2v2 = Cake.from_bytes(b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. v2')
        >>> o3 = CakeRack().cake()
        >>> r1 = CakeRack()
        >>> r1['o1']=o1
        >>> r1['o2']=o2v1
        >>> r1['o3']=None
        >>> r2 = CakeRack()
        >>> r2['o1']=o1
        >>> r2['o2']=o2v2
        >>> r2['o3']=o3
        >>> list(r2.merge(r1))
        [(<PatchAction.update: 1>, 'o2', Cake('NlXF0MZtHOZ3EE0Z2zPz80I9YG7vbN7KAbm1qJv3EZ50'))]
        >>> list(r1.merge(r2))
        [(<PatchAction.update: 1>, 'o2', Cake('zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT0'))]
        >>> r1['o1'] = None
        >>> list(r2.merge(r1)) #doctest: +NORMALIZE_WHITESPACE
        [(<PatchAction.delete: -1>, 'o1', None),
        (<PatchAction.update: 1>, 'o1', Cake('l01natqrQGg1ueJkFIc9mUYt18gcJjdsPLSLyzGgjY70')),
        (<PatchAction.update: 1>, 'o2', Cake('NlXF0MZtHOZ3EE0Z2zPz80I9YG7vbN7KAbm1qJv3EZ50'))]
        >>> list(r1.merge(r2)) #doctest: +NORMALIZE_WHITESPACE
        [(<PatchAction.delete: -1>, 'o1', None),
        (<PatchAction.update: 1>, 'o1', None),
        (<PatchAction.update: 1>, 'o2', Cake('zQQN0yLEZ5dVzPWK4jFifOXqnjgrQLac7T365E1ckGT0'))]
        >>> del r1["o2"]
        >>> list(r2.merge(r1)) #doctest: +NORMALIZE_WHITESPACE
        [(<PatchAction.delete: -1>, 'o1', None),
        (<PatchAction.update: 1>, 'o1', Cake('l01natqrQGg1ueJkFIc9mUYt18gcJjdsPLSLyzGgjY70')),
        (<PatchAction.update: 1>, 'o2', Cake('NlXF0MZtHOZ3EE0Z2zPz80I9YG7vbN7KAbm1qJv3EZ50'))]
        >>> list(r1.merge(r2)) #doctest: +NORMALIZE_WHITESPACE
        [(<PatchAction.delete: -1>, 'o1', None),
        (<PatchAction.update: 1>, 'o1', None),
        (<PatchAction.delete: -1>, 'o2', None)]
        """
        for k in sorted(list(set(self.keys() + previous.keys()))):
            if k not in self and k in previous:
                yield PatchAction.delete, k, None
            else:
                v = self[k]
                neuron = self.is_neuron(k)
                if k in self and k not in previous:
                    yield PatchAction.update, k, v
                else:
                    prev_v = previous[k]
                    prev_neuron = previous.is_neuron(k)
                    if v != prev_v:
                        if neuron == True and prev_neuron == True:
                            continue
                        if prev_neuron == neuron:
                            yield PatchAction.update, k, v
                        else:
                            yield PatchAction.delete, k, None
                            yield PatchAction.update, k, v

    def is_neuron(self, k) -> Optional[bool]:
        v = self.store[k]
        return v is None or v.is_folder

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

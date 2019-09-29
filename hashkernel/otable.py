from typing import Any, Dict, List, Optional, Union

from hashkernel import (
    Conversion,
    DictLike,
    Template,
    ensure_string,
    json_decode,
    json_encode,
    not_zero_len,
)
from hashkernel.smattr import Mold, SmAttr


class ORow:
    def _row_id(self) -> int:
        raise AssertionError("need to be implemented")


def get_row_id(row_id: Union[int, ORow]) -> int:
    if isinstance(row_id, int):
        return row_id
    return row_id._row_id()


class OTable(metaclass=Template):
    """
    >>> from datetime import date, datetime
    >>> class A(SmAttr):
    ...     i:int
    ...     s:str = 'xyz'
    ...     d:Optional[datetime]
    ...     z:List[datetime]
    ...     y:Dict[str,str]
    ...
    >>> t = OTable[A]()
    >>> str(t)
    '#{"columns": ["i", "s", "d", "z", "y"]}\\n'
    >>> t.add_row(A(i=5,s='abc'))
    0
    >>> str(t)
    '#{"columns": ["i", "s", "d", "z", "y"]}\\n[5, "abc", null, [], {}]\\n'
    >>> t.find_invalid_keys(t.add_row([7,None,'2018-08-10',None,None]))
    []
    >>> t.add_row([])
    Traceback (most recent call last):
    ...
    AttributeError: arrays has to match in size: ['i', 's', 'd', 'z', 'y'] []
    >>> t.add_row([None,None,None,None,None])
    Traceback (most recent call last):
    ...
    hashkernel.smattr.ValueRequired: no default for Required[int]
    error in i:Required[int]
    >>> str(t)
    '#{"columns": ["i", "s", "d", "z", "y"]}\\n[5, "abc", null, [], {}]\\n[7, "xyz", "2018-08-10T00:00:00", [], {}]\\n'
    >>> t = OTable(str(t),A)
    >>> str(t)
    '#{"columns": ["i", "s", "d", "z", "y"]}\\n[5, "abc", null, [], {}]\\n[7, "xyz", "2018-08-10T00:00:00", [], {}]\\n'
    >>> OTable[A]('a')
    Traceback (most recent call last):
    ...
    AttributeError: header should start with "#": a
    >>> t.find_invalid_rows()
    []
    >>> r=t.new_row()
    >>> t.find_invalid_rows()
    [2]
    >>> t.find_invalid_keys(r)
    ['i', 's', 'z', 'y']
    >>> t.find_invalid_keys(2)
    ['i', 's', 'z', 'y']
    >>> r.i
    >>> r.i=77
    >>> r.i
    77
    >>> r[3]=[datetime(2018,8,1),]
    >>> t.find_invalid_keys(r)
    ['s', 'y']
    >>> r['y']={}
    >>> r['y']
    {}
    >>> r[4]
    {}
    >>> t.find_invalid_rows()
    [2]
    >>> str(t)
    '#{"columns": ["i", "s", "d", "z", "y"]}\\n[5, "abc", null, [], {}]\\n[7, "xyz", "2018-08-10T00:00:00", [], {}]\\n[77, null, null, ["2018-08-01T00:00:00"], {}]\\n'
    >>> len(t)
    3
    >>> str(OTable[A](str(t)))
    '#{"columns": ["i", "s", "d", "z", "y"]}\\n[5, "abc", null, [], {}]\\n[7, "xyz", "2018-08-10T00:00:00", [], {}]\\n[77, "xyz", null, ["2018-08-01T00:00:00"], {}]\\n'
    >>> r['s']='zyx'
    >>> str(OTable[A](str(t)))
    '#{"columns": ["i", "s", "d", "z", "y"]}\\n[5, "abc", null, [], {}]\\n[7, "xyz", "2018-08-10T00:00:00", [], {}]\\n[77, "zyx", null, ["2018-08-01T00:00:00"], {}]\\n'
    """

    def __init__(self, s: Union[str, bytes, None] = None, mold: Any = None) -> None:
        if mold is not None:
            self.mold = Mold.ensure_it(mold)
        else:
            self.mold = Mold.ensure_it(type(self).__item_cref__.cls)  # type: ignore
        self.data: List[List[Any]] = []
        if s is not None:
            s = ensure_string(s)
            lines = filter(not_zero_len, (s.strip() for s in s.split("\n")))
            header_line = next(lines)
            if header_line[0] != "#":
                raise AttributeError(f'header should start with "#":' f" {header_line}")
            header = json_decode(header_line[1:])
            cols = tuple(header["columns"])
            # TODO support: [ int, bool, str, float, date, datetime, object ]
            mold_cols = tuple(self.mold.keys)
            if mold_cols != cols:
                raise AttributeError(f" mismatch: {cols} {mold_cols}")
            for l in lines:
                self.add_row(json_decode(l))

    def add_row(self, row=None):
        if not isinstance(row, (list, tuple)):
            if not isinstance(row, dict):
                row = DictLike(row)
            row = self.mold.dict_to_row(row)
        row = self.mold.mold_row(row, Conversion.TO_OBJECT)
        row_id = len(self.data)
        self.data.append(row)
        return row_id

    def __len__(self):
        return len(self.data)

    def __getitem__(self, row_id: int) -> ORow:
        mold = self.mold
        row = self.data[row_id]

        class _MoldedRow(ORow):
            def _row_id(self):
                return row_id

            def __getattr__(self, key):
                return row[mold.attrs[key].index]

            def __setattr__(self, key, value):
                row[mold.attrs[key].index] = value

            def __getitem__(self, item):
                if isinstance(item, str):
                    return self.__getattr__(item)
                else:
                    return row[item]

            def __setitem__(self, key, value):
                if isinstance(key, str):
                    self.__setattr__(key, value)
                else:
                    row[key] = value

        return _MoldedRow()

    def find_invalid_keys(self, row_id: Union[int, ORow]) -> List[str]:
        row_id = get_row_id(row_id)
        invalid_keys = []
        for ae in self.mold.attrs.values():
            if not (ae.validate(self.data[row_id][ae.index])):
                invalid_keys.append(ae.name)
        return invalid_keys

    def find_invalid_rows(self) -> List[int]:
        return [
            row_id
            for row_id in range(len(self.data))
            if len(self.find_invalid_keys(row_id)) > 0
        ]

    def new_row(self) -> ORow:
        row_id = len(self.data)
        self.data.append([None for _ in self.mold.keys])
        return self[row_id]

    def __str__(self):
        def gen():
            yield "#" + json_encode({"columns": self.mold.keys})
            for row in self.data:
                yield json_encode(self.mold.mold_row(row, Conversion.TO_JSON))
            yield ""

        return "\n".join(gen())

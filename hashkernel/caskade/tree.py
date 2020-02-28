from pathlib import Path
from typing import Union

from hashkernel.files import ensure_path


class TreeCaskade:
    dir:Path

    def __init__(self, dir:Union[Path,str]):
        self.dir = ensure_path(dir)

    def _hash_tree(self)->Path:
        return self.dir / '.hash_tree'




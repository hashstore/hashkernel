from pathlib import Path
from typing import Union

from hs_build_tools import ensure_dir

from hashkernel.tests import BytesGen


def seed_file(dir: Path, seed, sz):
    ensure_dir(str(dir))
    file = dir / f"{seed}_{sz}.dat"
    if not file.exists():
        bg = BytesGen(seed)
        file.open("wb").write(bg.get_bytes(sz))
    return file


def dump_file(file: Path, content: Union[str, bytes]):
    dir = file.parent
    ensure_dir(str(dir))
    if isinstance(content, str):
        content = content.encode("utf-8")
    file.open("wb").write(content)
    return file

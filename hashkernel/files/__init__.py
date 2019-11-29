import asyncio
from pathlib import Path
from typing import Any, Callable


def ensure_path(path: Any) -> Path:
    if isinstance(path, Path):
        return path
    return Path(path)


def read_text(path: Path, process: Callable[[Path, str], None]):
    process(path, path.open("rt").read())


async def aio_read_text(path: Path, process: Callable[[Path, str], None]):
    await asyncio.get_event_loop().run_in_executor(None, read_text, path, process)


PathFilter = Callable[[Path], bool]


def any_path(_: Path):
    return True

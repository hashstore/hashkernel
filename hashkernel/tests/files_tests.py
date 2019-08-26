from pathlib import Path

import pytest
from hs_build_tools import LogTestOut

from hashkernel.files import FileBytes
from hashkernel.tests import BytesGen

log, out = LogTestOut.get(__name__)

FILE_SZ = (1 << 20) + 25


def ensure_file():
    file_bytes = Path(out.child_dir("file_bytes"))
    file = file_bytes / "bigrandom.dat"
    if not file.exists():
        bg = BytesGen(0)
        file.open("wb").write(bg.get_bytes(FILE_SZ))
    return file

@pytest.mark.slow
def test_file_bytes():
    file = ensure_file()
    file_bytes = FileBytes(file, 2)
    assert len(file_bytes) == FILE_SZ
    assert file_bytes[0] == 0x0C5
    b = file_bytes[0x3FFF:0x40FF]
    assert len(b) == 1 << 8
    assert b[:2].hex() == "beb2"
    assert (
        str(file_bytes.load_segment.cache_info())
        == "CacheInfo(hits=1, misses=2, maxsize=2, currsize=2)"
    )

    assert file_bytes[-3] == 0x61
    b64k = file_bytes[0x0FFF0:0x1FFF0]
    assert len(b64k) == 1 << 16
    """
    000fff0 08 6b ca 97 cd 59 bf bf 03 86 2e 0b bb 0a 74 25
    0010000 89 ed 9b 97 f4 86 2d f5 28 d8 5e 29 cf 39 53 d3
    """
    assert b64k[:16].hex() == "086bca97cd59bfbf03862e0bbb0a7425"
    assert b64k[16:32].hex() == "89ed9b97f4862df528d85e29cf3953d3"

    assert file_bytes[-FILE_SZ] == 0x0C5
    with pytest.raises(IndexError, match="index out of range"):
        file_bytes[FILE_SZ]
    with pytest.raises(IndexError, match="index out of range"):
        file_bytes[-1-FILE_SZ]
    with pytest.raises(KeyError, match="Not sure what to do with"):
        file_bytes["a"]

    assert file_bytes[FILE_SZ+5:] == b''
    assert file_bytes[-3:-2] == b'\x61'

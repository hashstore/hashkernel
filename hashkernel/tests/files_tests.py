from pathlib import Path

import pytest
from hs_build_tools import LogTestOut

from hashkernel.files import FileBytes
from hashkernel.packer import INT_8, INT_16, INT_32, INT_64, BE_INT_64, \
    FLOAT, DOUBLE, NeedMoreBytes
from hashkernel.tests import BytesGen

log, out = LogTestOut.get(__name__)



def ensure_file(seed,sz):
    file_bytes = Path(out.child_dir("file_bytes"))
    file = file_bytes / f"{seed}_{sz}.dat"
    if not file.exists():
        bg = BytesGen(seed)
        file.open("wb").write(bg.get_bytes(sz))
    return file

@pytest.mark.slow
def test_file_bytes():
    SZ = (1 << 20) + 25
    file = ensure_file(0,SZ)
    file_bytes = FileBytes(file, 2)
    assert len(file_bytes) == SZ
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

    assert file_bytes[-SZ] == 0x0C5
    with pytest.raises(IndexError, match="index out of range"):
        file_bytes[SZ]
    with pytest.raises(IndexError, match="index out of range"):
        file_bytes[-1-SZ]
    with pytest.raises(KeyError, match="Not sure what to do with"):
        file_bytes["a"]

    assert file_bytes[SZ+5:] == b''
    assert file_bytes[-3:-2] == b'\x61'


def test_file_bytes_with_type_packer():
    SZ = 0x1000A # 64k + 10
    file = ensure_file(0, SZ)
    file_bytes = FileBytes(file, 2)
    assert len(file_bytes) == SZ
    assert file_bytes[0] == 0x0C5
    b = file_bytes[0x3FFF:0x40FF]
    assert len(b) == 1 << 8
    assert b[:2].hex() == "beb2"
    assert (
        str(file_bytes.load_segment.cache_info())
        == "CacheInfo(hits=1, misses=2, maxsize=2, currsize=2)"
    )


    """
    000ffe0 c4 7e 6a 5d d5 26 ba 85 b9 a3 9f 02 20 1c 36 2d
    000fff0 08 6b ca 97 cd 59 bf bf 03 86 2e 0b bb 0a 74 25
    0010000 89 ed 9b 97 f4 86 2d f5 28 d8                  
    001000a
    """
    assert INT_8.unpack(file_bytes,0) == (0x0C5, 1)
    assert INT_16.unpack(file_bytes,SZ-2) == (0x0d828, SZ)
    with pytest.raises(NeedMoreBytes):
        INT_16.unpack(file_bytes,SZ-1)
    assert INT_32.unpack(file_bytes,SZ-INT_32.size) == (0x0d828f52d, SZ)
    assert INT_64.unpack(file_bytes,SZ-INT_64.size-1) == (0x028f52d86f4979bed, SZ-1)
    assert BE_INT_64.unpack(file_bytes,SZ-BE_INT_64.size-1) == (0x0ed9b97f4862df528, SZ-1)
    assert FLOAT.unpack(file_bytes,SZ-FLOAT.size) == (-743083901714432.0, SZ)
    assert DOUBLE.unpack(file_bytes,SZ-DOUBLE.size) == (-4.9169223605762894e+116, SZ)



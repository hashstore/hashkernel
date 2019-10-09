from datetime import datetime
from logging import getLogger
from random import Random

import pytest

import hashkernel.packer as p

log = getLogger(__name__)

r = Random()
r.seed()


def test_1():
    begining_of_time = datetime.utcfromtimestamp(0.0)
    z = p.TuplePacker(
        p.UTC_DATETIME, p.DOUBLE, p.UTF8_STR, p.FLOAT, p.INT_32, p.INT_16, p.INT_8
    )
    pack = z.pack(
        (begining_of_time, 13497439e-30, "Hello World!!!", 42.0, 1000, 1000, 244)
    )
    assert (
        pack.hex() == "0000000000000000"
        "29f7654a4151303b"
        "8e48656c6c6f20576f726c64212121"
        "00002842"
        "e8030000"
        "e803"
        "f4"
    )
    unpack, sz = z.unpack(pack, 0)
    assert unpack == (
        begining_of_time,
        1.3497439e-23,
        "Hello World!!!",
        42.0,
        1000,
        1000,
        244,
    )
    assert len(pack) == sz

    z = p.TuplePacker(p.FixedSizePacker(3), p.UTC_DATETIME, p.INT_8)
    pack = z.pack((bytes([65, 66, 67]), begining_of_time, 17))
    assert pack.hex() == "414243" "0000000000000000" "11"
    unpack, sz = z.unpack(pack, 0)
    assert unpack == (b"ABC", begining_of_time, 17)
    assert len(pack) == sz


@pytest.mark.parametrize(
    "packer, max_capacity",
    [(p.ADJSIZE_PACKER_3, 2 ** 21), (p.ADJSIZE_PACKER_4, 2 ** 28)],
)
def test_adjsize_packers(packer: p.AdjustableSizePacker, max_capacity: int):

    oks = [0, 1, max_capacity - 1, *(r.randrange(max_capacity) for _ in range(100))]
    for i in oks:
        n, _ = packer.unpack(packer.pack(i), 0)
        assert i == n

    fails = (
        max_capacity,
        max_capacity + 1,
        *(r.randrange(max_capacity, 2 * max_capacity) for _ in range(20)),
    )
    for i in fails:
        with pytest.raises(ValueError, match="Size is too big"):
            packer.pack(i)

    def set_mask_bit(buff: bytes):
        mask_bit = buff[-1]
        if len(buff) > 1 and mask_bit == 0:
            mask_bit = 1
        return buff[:-1] + bytes([p.MARK_BIT.mask | mask_bit])

    max_size = packer.max_size
    ok_bytes = [
        set_mask_bit(b"\x00"),
        set_mask_bit(b"\x7f" * max_size),
        *(
            set_mask_bit(
                bytes([r.randrange(128) for _ in range(1 + r.randrange(max_size))])
            )
            for _ in range(100)
        ),
    ]

    for buff in ok_bytes:
        assert isinstance(buff, bytes)
        i, offset = packer.unpack(buff, 0)
        assert offset == len(buff)
        assert packer.pack(i) == buff

    toobig_bytes = [
        b"\x00" * max_size,
        b"\x7f" * max_size,
        set_mask_bit(b"\x00" * (max_size + 1)),
        set_mask_bit(b"\x7f" * (max_size + 1)),
        *(
            set_mask_bit(
                bytes(
                    [
                        r.randrange(128)
                        for _ in range(r.randrange(max_size + 1, max_size * 2))
                    ]
                )
            )
            for _ in range(100)
        ),
    ]

    for buff in toobig_bytes:
        assert isinstance(buff, bytes)
        with pytest.raises(ValueError, match="No end bit"):
            packer.unpack(buff, 0)

import hashkernel.base_x as bx
from hs_build_tools.pytest import eq_,ok_
from random import seed, randint


def random_bytes(sz):
    return bytes( randint(0,255) for _ in range(sz) )


b58 = bx.base_x(58)


def test_nulls():
    eq_(b58.decode('12'), b'\x00\x01')
    eq_(b58.decode(b'12'), b'\x00\x01')
    eq_(b58.encode(b'\0\1'),'12')
    eq_(b58.decode('1'), b'\x00')
    eq_(b58.encode(b'\0'),'1')
    eq_(b58.decode(''), b'')
    eq_(b58.encode(b''),'')
    try:
        b58.encode('')
        ok_(False)
    except TypeError:
        pass


def test_randomized():
    all_codecs = [bx.base_x(k) for k in bx.alphabets]

    seed(0)
    for sz in [1, 2, 0, 3, 1, 77, 513, 732]:
        b = random_bytes(sz)

        for codec in all_codecs:
            s = codec.encode(b)
            eq_(codec.decode(s), b)
            s = codec.encode_check(b)
            eq_(codec.decode_check(s), b)

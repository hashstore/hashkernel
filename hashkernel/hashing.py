import abc
import base64
import os
from hashlib import sha1, sha256
from typing import IO, Callable, Optional

from hashkernel import EnsureIt, Stringable, ensure_bytes, ensure_string
from hashkernel.base_x import base_x

B36 = base_x(36)

ALGO = sha256


class Hasher:
    """
    >>> Hasher().digest()
    b"\\xe3\\xb0\\xc4B\\x98\\xfc\\x1c\\x14\\x9a\\xfb\\xf4\\xc8\\x99o\\xb9$'\\xaeA\\xe4d\\x9b\\x93L\\xa4\\x95\\x99\\x1bxR\\xb8U"
    >>> Hasher(b"Hello").digest()
    b'\\x18_\\x8d\\xb3"q\\xfe%\\xf5a\\xa6\\xfc\\x93\\x8b.&C\\x06\\xec0N\\xdaQ\\x80\\x07\\xd1vH&8\\x19i'
    >>> Hasher.SIZEOF
    32
    """

    SIZEOF = len(ALGO().digest())

    def __init__(
        self,
        data: Optional[bytes] = None,
        on_update: Optional[Callable[[bytes], None]] = None,
    ) -> None:
        self.sha = ALGO()
        self.on_update = on_update
        if data is not None:
            self.update(data)

    def update(self, b: bytes) -> "Hasher":
        self.sha.update(b)
        if self.on_update is not None:
            self.on_update(b)
        return self

    def update_from_stream(self, fd: IO[bytes], chunk_size: int = 65355) -> "Hasher":
        while True:
            chunk = fd.read(chunk_size)
            if len(chunk) <= 0:
                break
            self.update(chunk)
        fd.close()
        return self

    def digest(self) -> bytes:
        return self.sha.digest()


def shard_name_int(num: int):
    """
    >>> shard_name_int(0)
    '0'
    >>> shard_name_int(1)
    '1'
    >>> shard_name_int(8000)
    '668'
    """
    return B36.encode_int(num)


def decode_shard(name: str):
    """
    >>> decode_shard('0')
    0
    >>> decode_shard('668')
    8000
    """
    return B36.decode_int(name)


def is_it_shard(shard_name: str, max_num: int) -> bool:
    """
    Test if name can represent shard

    >>> is_it_shard('668', 8192)
    True
    >>> is_it_shard('6bk', 8192)
    False
    >>> is_it_shard('0', 8192)
    True

    logic should not be sensitive for upper case:
    >>> is_it_shard('5BK', 8192)
    True
    >>> is_it_shard('6BK', 8192)
    False
    >>> is_it_shard('', 8192)
    False
    >>> is_it_shard('.5k', 8192)
    False
    >>> is_it_shard('abcd', 8192)
    False
    """
    shard_num = -1
    if shard_name == "" or len(shard_name) > 3:
        return False
    try:
        shard_num = decode_shard(shard_name.lower())
    except:
        pass
    return shard_num >= 0 and shard_num < max_num


def shard_based_on_two_bites(digest: bytes, base: int) -> int:
    """
    >>> shard_based_on_two_bites(b'ab', 7)
    3
    """
    b1, b2 = digest[:2]
    return (b1 * 256 + b2) % base


_SSHA_MARK = "{SSHA}"


class SaltedSha(Stringable, EnsureIt):
    """
    >>> ssha = SaltedSha.from_secret('abc')
    >>> ssha.check_secret('abc')
    True
    >>> ssha.check_secret('zyx')
    False
    >>> ssha = SaltedSha('{SSHA}5wRHUQxypw7C4AVd4yZRW/8pXy2Gwvh/')
    >>> ssha.check_secret('abc')
    True
    >>> ssha.check_secret('Abc')
    False
    >>> ssha.check_secret('zyx')
    False
    >>> str(ssha)
    '{SSHA}5wRHUQxypw7C4AVd4yZRW/8pXy2Gwvh/'
    >>> ssha
    SaltedSha('{SSHA}5wRHUQxypw7C4AVd4yZRW/8pXy2Gwvh/')

    """

    def __init__(
        self, s: Optional[str], _digest: bytes = None, _salt: bytes = None
    ) -> None:
        if s is None:
            self.digest = _digest
            self.salt = _salt
        else:
            len_of_mark = len(_SSHA_MARK)
            if _SSHA_MARK == s[:len_of_mark]:
                challenge_bytes = base64.b64decode(s[len_of_mark:])
                self.digest = challenge_bytes[:20]
                self.salt = challenge_bytes[20:]
            else:
                raise AssertionError("cannot init: %r" % s)

    @staticmethod
    def from_secret(secret):
        secret = ensure_bytes(secret)
        h = sha1(secret)
        salt = os.urandom(4)
        h.update(salt)
        return SaltedSha(None, _digest=h.digest(), _salt=salt)

    def check_secret(self, secret):
        secret = ensure_bytes(secret)
        h = sha1(secret)
        h.update(self.salt)
        return self.digest == h.digest()

    def __str__(self):
        encode = base64.b64encode(self.digest + self.salt)
        return _SSHA_MARK + ensure_string(encode)


class InetAddress(Stringable, EnsureIt):
    """
    >>> InetAddress('127.0.0.1')
    InetAddress('127.0.0.1')
    """

    def __init__(self, k):
        self.k = k

    def __str__(self):
        return self.k

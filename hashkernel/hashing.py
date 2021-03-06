import abc
import base64
import os
from functools import total_ordering
from hashlib import sha1, sha256
from io import BytesIO
from pathlib import Path
from typing import IO, Callable, ClassVar, Optional, Union

from hashkernel import (
    EnsureIt,
    Primitive,
    StrigableFactory,
    Stringable,
    ensure_bytes,
    ensure_string,
)
from hashkernel.base_x import base_x
from hashkernel.files import ensure_path
from hashkernel.packer import FixedSizePacker, Packer, ProxyPacker

B36 = base_x(36)
B62 = base_x(62)


class B36_Mixin:
    """
    Usefull for filenames. Some file systems are case-insesitive.
    """

    @classmethod
    def from_b36(cls, s: str):
        return cls(B36.decode(s.lower()))  # type: ignore

    def to_b36(self):
        return B36.encode(bytes(self))


class BytesOrderingMixin:
    """ use bytes() to define orgering
        ned to be used with @total_ordering
    """

    def __eq__(self, other) -> bool:
        return bytes(self) == bytes(other)  # type: ignore

    def __lt__(self, other) -> bool:
        return bytes(self) < bytes(other)  # type: ignore


ALGO = sha256


class Hasher:
    """
    >>> Hasher().digest()
    b"\\xe3\\xb0\\xc4B\\x98\\xfc\\x1c\\x14\\x9a\\xfb\\xf4\\xc8\\x99o\\xb9$'\\xaeA\\xe4d\\x9b\\x93L\\xa4\\x95\\x99\\x1bxR\\xb8U"
    >>> Hasher().update(b"Hello").digest()
    b'\\x18_\\x8d\\xb3"q\\xfe%\\xf5a\\xa6\\xfc\\x93\\x8b.&C\\x06\\xec0N\\xdaQ\\x80\\x07\\xd1vH&8\\x19i'
    >>> Hasher.SIZEOF
    32
    """

    SIZEOF = len(ALGO().digest())

    def __init__(self, on_update: Optional[Callable[[bytes], None]] = None) -> None:
        self.sha = ALGO()
        self.on_update = on_update

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


class Signer(StrigableFactory):
    def signature_size(self) -> int:
        raise AssertionError("need to be implemented")

    def sign(self, buffer: bytes) -> bytes:
        raise AssertionError("need to be implemented")

    def validate(self, buffer: bytes, signature: bytes) -> bool:
        raise AssertionError("need to be implemented")


class HasherSigner(Signer):
    """
    primitive signer that assumes that both party
    share same secret

    >>> signer = HasherSigner()
    >>> signer.signature_size()
    32
    >>> signer.sign(b'abc')
    Traceback (most recent call last):
    ...
    ValueError: secret is not initialized
    >>> signer.validate(b'abc', b'xyz')
    Traceback (most recent call last):
    ...
    ValueError: secret is not initialized
    >>>

    For example that allow validate password without sending
    it over insecure line

    >>> server_challenge = os.urandom(16)

    Server sends some random challenge to client. Client sign that
    gibberish and send it back.

    >>> signer.init(b'password')
    HasherSigner('HASHER')
    >>> signature = signer.sign(server_challenge)


    Server validates signature

    >>> server_signer = HasherSigner().init(b'password')
    >>> server_signer.validate(server_challenge, signature)
    True


    """

    secret: Optional[bytes] = None

    def signature_size(self):
        return Hasher.SIZEOF

    def init(self, secret) -> "HasherSigner":
        self.secret = secret
        return self

    def sign(self, buffer: bytes) -> bytes:
        if self.secret is None:
            raise ValueError("secret is not initialized")
        return Hasher().update(buffer).update(self.secret).digest()

    def validate(self, buffer: bytes, signature: bytes) -> bool:
        return self.sign(buffer) == signature


Signer.register("HASHER", HasherSigner)

import abc
from typing import Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa, dsa
from cryptography.hazmat.primitives.serialization import (
    BestAvailableEncryption,
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
    load_pem_private_key,
    load_ssh_public_key,
)

from hashkernel import EnsureIt, StrigableFactory


class PublicKey(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __bytes__(self) -> bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def verify(self, message: bytes, signature: bytes):
        raise NotImplementedError("subclasses must override")


class EncryptionKey(PublicKey):
    @abc.abstractmethod
    def encrypt(self, message: bytes) -> bytes:
        raise NotImplementedError("subclasses must override")


class PrivateKey(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def pub(self) -> PublicKey:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def private_bytes(self, password:Optional[bytes] = None)  -> bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def sign(self, message: bytes) -> bytes:
        raise NotImplementedError("subclasses must override")

class DecryptionKey(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def decrypt(self, ciphertext) -> bytes:
        raise NotImplementedError("subclasses must override")


class Algorithm(StrigableFactory):

    def load_private_key(
        self, buffer: bytes, password: Optional[bytes] = None
    ) -> PrivateKey:
        raise NotImplementedError("subclasses must override")

    def load_public_key(self, buffer: bytes) -> PublicKey:
        raise NotImplementedError("subclasses must override")

    def generate_private_key(self) -> PrivateKey:
        raise NotImplementedError("subclasses must override")




def _pss_padding():
    return (
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256(),
    )


def _oaep_padding():
    return padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None,
    )




class RsaPublicKey(EncryptionKey, EnsureIt):
    def __init__(self, inst):
        if isinstance(inst, bytes):
            self.inst = load_ssh_public_key(inst, default_backend())
        else:
            self.inst = inst

    def __bytes__(self) -> bytes:
        return self.inst.public_bytes(
            encoding=Encoding.OpenSSH, format=PublicFormat.OpenSSH
        )

    def verify(self, message: bytes, signature: bytes):
        self.inst.verify(signature, message, *_pss_padding())

    def encrypt(self, message: bytes) -> bytes:
        return self.inst.encrypt(message, _oaep_padding())


class RsaPrivateKey(DecryptionKey):
    def __init__(self, inst, password: Optional[bytes] = None):
        if isinstance(inst, bytes):
            self.inst = load_pem_private_key(inst, password, default_backend())
        else:
            self.inst = inst

    def private_bytes(self, password: Optional[bytes] = None) -> bytes:
        if password is None:
            return self.inst.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption(),
            )
        else:
            return self.inst.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=BestAvailableEncryption(password),
            )

    def pub(self):
        return RsaPublicKey(self.inst.public_key())

    def sign(self, message: bytes) -> bytes:
        return self.inst.sign(message, *_pss_padding())


    def decrypt(self, ciphertext: bytes) -> bytes:
        return self.inst.decrypt(ciphertext, _oaep_padding())



class RSA2048(Algorithm):
    """
    >>> scheme = RSA2048()
    >>> k = scheme.generate_private_key()
    >>> import os
    >>> sigs = [k.sign(os.urandom(sz)) for sz in range(0, 1000, 25)]
    >>> min(map(len,sigs)), max(map(len,sigs))
    (256, 256)
    """

    def load_private_key(
        self, buffer: bytes, password: Optional[bytes] = None
    ) :
        return RsaPrivateKey(buffer, password)

    def load_public_key(self, buffer: bytes) :
        return RsaPublicKey(buffer)

    def generate_private_key(self) :
        return RsaPrivateKey(
            rsa.generate_private_key(
                public_exponent=65537, key_size=2048, backend=default_backend()
            )
        )



Algorithm.register(RSA2048)

class DsaPublicKey(PublicKey, EnsureIt):
    def __init__(self, inst):
        if isinstance(inst, bytes):
            self.inst = load_ssh_public_key(inst, default_backend())
        else:
            self.inst = inst

    def __bytes__(self) -> bytes:
        return self.inst.public_bytes(
            encoding=Encoding.OpenSSH, format=PublicFormat.OpenSSH
        )

    def verify(self, message: bytes, signature: bytes):
        self.inst.verify(signature, message, hashes.SHA256())



class DsaPrivateKey(PrivateKey):
    def __init__(self, inst, password: Optional[bytes] = None):
        if isinstance(inst, bytes):
            self.inst = load_pem_private_key(inst, password, default_backend())
        else:
            self.inst = inst

    def private_bytes(self, password: Optional[bytes] = None) -> bytes:
        if password is None:
            return self.inst.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption(),
            )
        else:
            return self.inst.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=BestAvailableEncryption(password),
            )

    def pub(self):
        return DsaPublicKey(self.inst.public_key())

    def sign(self, message: bytes) -> bytes:
        return self.inst.sign(message, hashes.SHA256())




class DSA2048(Algorithm):
    """
    >>> scheme = DSA2048()
    >>> k = scheme.generate_private_key()
    >>> import os
    >>> sigs = [k.sign(os.urandom(sz)) for sz in range(0, 1000, 25)]
    """

    def load_private_key(
        self, buffer: bytes, password: Optional[bytes] = None
    ) :
        return DsaPrivateKey(buffer, password)

    def load_public_key(self, buffer: bytes) :
        return DsaPublicKey(buffer)

    def generate_private_key(self) :
        return DsaPrivateKey(
            dsa.generate_private_key(
                key_size=2048, backend=default_backend()
            )
        )

Algorithm.register(DSA2048)

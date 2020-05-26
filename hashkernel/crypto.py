import abc
from typing import Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.serialization import PublicFormat, \
    PrivateFormat, Encoding, load_pem_private_key, \
    load_ssh_public_key, NoEncryption, BestAvailableEncryption


class PublicKey(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __bytes__(self)->bytes:
        raise NotImplementedError("subclasses must override")


class PrivateKey(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def pub(self)-> PublicKey:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def private_bytes(self, p)->bytes:
        raise NotImplementedError("subclasses must override")


class EncryptionScheme(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def load_private_key(self, buffer: bytes, password:Optional[bytes] = None)->PrivateKey:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def load_public_key(self, buffer: bytes)->PublicKey:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def generate_private_key(self)->PrivateKey:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def sign(self, message: bytes, key: PrivateKey)->bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def verify(self, message: bytes, signature: bytes, pub_key: PublicKey):
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def encrypt(self, message, pub_key: PublicKey)->bytes:
        raise NotImplementedError("subclasses must override")

    @abc.abstractmethod
    def decrypt(self, ciphertext, private_key: PrivateKey)->bytes:
        raise NotImplementedError("subclasses must override")

def _pss_padding():
    return (
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256(),
    )

def _oaep_padding():
    return padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None,
    )

class RsaPublicKey(PublicKey):
    def __init__(self, inst):
        if isinstance(inst, bytes):
            self.inst = load_ssh_public_key(inst, default_backend() )
        else:
            self.inst = inst

    def __bytes__(self)->bytes:
        return self.inst.public_bytes(encoding=Encoding.OpenSSH, format=PublicFormat.OpenSSH)


class RsaPrivateKey(PrivateKey):

    def __init__(self, inst, password:Optional[bytes] = None):
        if isinstance(inst, bytes):
            self.inst = load_pem_private_key(inst, password, default_backend())
        else:
            self.inst = inst

    def private_bytes(self, password:Optional[bytes] = None) -> bytes:
        if password is None:
            return self.inst.private_bytes(encoding=Encoding.PEM,
                                           format=PrivateFormat.PKCS8,
                                           encryption_algorithm=NoEncryption())
        else:
            return self.inst.private_bytes(encoding=Encoding.PEM,
                                           format=PrivateFormat.TraditionalOpenSSL,
                                           encryption_algorithm=BestAvailableEncryption(password))

    def pub(self):
        return RsaPublicKey(self.inst.public_key())


class RsaEncryptionScheme(EncryptionScheme):

    def load_private_key(self, buffer: bytes, password:Optional[bytes] = None)->RsaPrivateKey:
        return RsaPrivateKey(buffer, password)

    def load_public_key(self, buffer: bytes)->RsaPublicKey:
        return RsaPublicKey(buffer)

    def generate_private_key(self) -> PrivateKey:
        return RsaPrivateKey(rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        ))

    def sign(self, message: bytes, key)->bytes:
        return key.inst.sign(message, *_pss_padding())

    def verify(self, message:bytes, signature:bytes, pub_key):
        pub_key.inst.verify(signature, message, *_pss_padding())

    def encrypt(self, message:bytes, pub_key)->bytes:
        return pub_key.inst.encrypt(message, _oaep_padding())

    def decrypt(self, ciphertext:bytes, private_key)->bytes:
        return private_key.inst.decrypt(ciphertext, _oaep_padding())


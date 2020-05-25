from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import (padding, rsa)


def generate_private_key():
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend())


def sign(message, key):
    return key.sign(message, *_pss_padding())


def verify(message, signature, pub_key):
    pub_key.verify(signature, message, *_pss_padding())


def _pss_padding():
    return padding.PSS(
        mgf=padding.MGF1(hashes.SHA256()),
        salt_length=padding.PSS.MAX_LENGTH) , hashes.SHA256()


def _oaep_padding():
    return padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None
    )


def encrypt(message, pub_key):
    return pub_key.encrypt( message, _oaep_padding())


def decrypt(ciphertext, private_key):
    return private_key.decrypt( ciphertext, _oaep_padding())


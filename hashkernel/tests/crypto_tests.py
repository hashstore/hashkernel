from cryptography.fernet import Fernet

def test_symetric():
    key = Fernet.generate_key()
    f = Fernet(key)
    plaintext = b"A really secret message. Not for prying eyes."
    ciphertext = f.encrypt(plaintext)
    #at reciever
    f2 = Fernet(key)
    decrypted = f2.decrypt(ciphertext)
    assert decrypted == plaintext


from hashlib import sha256
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import (padding, rsa)


def test_asymetric():
    right_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend())

    wrong_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend())

    message = b"A message I want to sign"

    right_signature = right_key.sign(message, sha256_padding(), hashes.SHA256())

    wrong_signature = wrong_key.sign(message, sha256_padding(), hashes.SHA256())

    right_pkey = right_key.public_key()

    right_pkey.verify( right_signature, message, sha256_padding(), hashes.SHA256())

    try:
        right_pkey.verify( wrong_signature, message, sha256_padding(), hashes.SHA256())
        assert False
    except InvalidSignature:
        pass



def sha256_padding():
    return padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                       salt_length=padding.PSS.MAX_LENGTH)



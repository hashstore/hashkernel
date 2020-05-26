from cryptography.exceptions import InvalidSignature
from cryptography.fernet import Fernet

from hashkernel.crypto import RsaEncryptionScheme


def test_symetric():
    key = Fernet.generate_key()
    f = Fernet(key)
    plaintext = b"A really secret message. Not for prying eyes."
    ciphertext = f.encrypt(plaintext)
    # at reciever
    f2 = Fernet(key)
    decrypted = f2.decrypt(ciphertext)
    assert decrypted == plaintext


def test_sign():
    scheme = RsaEncryptionScheme()

    right_key = scheme.generate_private_key()

    wrong_key = scheme.generate_private_key()

    message = b"A message I want to sign"

    right_signature = scheme.sign(message, right_key)

    wrong_signature = scheme.sign(message, wrong_key)

    right_pkey = right_key.pub()

    scheme.verify(message, right_signature, right_pkey)

    try:
        scheme.verify(message + b"x", right_signature, right_pkey)
        assert False
    except InvalidSignature:
        pass
    try:
        scheme.verify(message, wrong_signature, right_pkey)
        assert False
    except InvalidSignature:
        pass


def test_crypt():
    scheme = RsaEncryptionScheme()

    right_key = scheme.generate_private_key()

    wrong_key = scheme.generate_private_key()

    right_pkey = right_key.pub()

    wrong_pkey = wrong_key.pub()

    message = b"A message I want to send"

    right_ciphertext = scheme.encrypt(message, right_pkey)

    wrong_ciphertext = scheme.encrypt(message, wrong_pkey)

    assert scheme.decrypt(right_ciphertext, right_key) == message

    try:
        scheme.decrypt(wrong_ciphertext, right_key)
        assert False
    except ValueError as e:
        assert str(e) == "Decryption failed."

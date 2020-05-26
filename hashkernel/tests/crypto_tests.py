import os

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

    do_verify(message, right_pkey, scheme, right_signature, wrong_signature)

    do_verify(
        message,
        scheme.load_public_key(bytes(right_pkey)),
        scheme,
        right_signature,
        wrong_signature,
    )


def do_verify(message, pubkey, scheme, right_signature, wrong_signature):
    scheme.verify(message, right_signature, pubkey)
    try:
        scheme.verify(message + b"x", right_signature, pubkey)
        assert False
    except InvalidSignature:
        pass
    try:
        scheme.verify(message, wrong_signature, pubkey)
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


def test_private_key_serialization():
    scheme = RsaEncryptionScheme()

    key = scheme.generate_private_key()
    pubkey = key.pub()

    key_no_pwd = scheme.load_private_key(key.private_bytes())
    pwd = os.urandom(5)

    key_pwd = scheme.load_private_key(key.private_bytes(pwd), pwd)

    message = b"A message I want to send"

    assert message == scheme.decrypt(scheme.encrypt(message, pubkey), key_no_pwd)
    assert message == scheme.decrypt(scheme.encrypt(message, pubkey), key_pwd)

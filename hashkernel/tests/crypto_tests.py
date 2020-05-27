import os

from cryptography.exceptions import InvalidSignature
from cryptography.fernet import Fernet

from hashkernel.crypto import RSA2048, Algorithm, DSA2048


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
    do_sign(RSA2048())
    do_sign(DSA2048())
    do_sign(Algorithm.ensure_it("DSA2048"))


def do_sign(algo):
    right_key = algo.generate_private_key()
    wrong_key = algo.generate_private_key()
    message = b"A message I want to sign"
    right_signature = right_key.sign(message)
    wrong_signature = wrong_key.sign(message)
    right_pkey = right_key.pub()
    do_verify(message, right_pkey, right_signature, wrong_signature)
    do_verify(
        message,
        algo.load_public_key(bytes(right_pkey)),
        right_signature,
        wrong_signature,
    )


def do_verify(message, pubkey, right_signature, wrong_signature):
    pubkey.verify(message, right_signature)
    try:
        pubkey.verify(message + b"x", right_signature)
        assert False
    except InvalidSignature:
        pass
    try:
        pubkey.verify(message, wrong_signature)
        assert False
    except InvalidSignature:
        pass


def test_crypt():
    do_crypt(RSA2048())
    do_crypt(Algorithm.ensure_it("RSA2048"))


def do_crypt(algo):
    right_key = algo.generate_private_key()
    wrong_key = algo.generate_private_key()
    right_pkey = right_key.pub()
    wrong_pkey = wrong_key.pub()
    message = b"A message I want to send"
    right_ciphertext = right_pkey.encrypt(message)
    wrong_ciphertext = wrong_pkey.encrypt(message)
    assert right_key.decrypt(right_ciphertext) == message
    try:
        right_key.decrypt(wrong_ciphertext)
        assert False
    except ValueError as e:
        assert str(e) == "Decryption failed."


def test_private_key_serialization():
    do_private_key_serialization(RSA2048())
    do_private_key_serialization(DSA2048())
    do_private_key_serialization(Algorithm.ensure_it("DSA2048"))



def do_private_key_serialization(algo):
    key = algo.generate_private_key()
    pubkey = key.pub()
    key_no_pwd = algo.load_private_key(key.private_bytes())
    pwd = os.urandom(5)
    key_pwd = algo.load_private_key(key.private_bytes(pwd), pwd)
    message = b"A message I want to send"
    pubkey.verify(message, key_no_pwd.sign(message))
    pubkey.verify(message, key_pwd.sign(message))

    #omit password
    try:
        algo.load_private_key(key.private_bytes(pwd))
        assert False
    except TypeError as e:
        assert str(e) == "Password was not given but private key is encrypted"

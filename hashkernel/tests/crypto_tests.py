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


from cryptography.exceptions import InvalidSignature
from hashkernel.crypto import (generate_private_key, sign,
                               verify, encrypt, decrypt)

def test_sign():
    right_key = generate_private_key()

    wrong_key = generate_private_key()

    message = b"A message I want to sign"

    right_signature = sign(message, right_key)

    wrong_signature = sign(message, wrong_key)

    right_pkey = right_key.public_key()

    verify(message, right_signature, right_pkey)

    try:
        verify(message+b'x', right_signature, right_pkey)
        assert False
    except InvalidSignature:
        pass
    try:
        verify(message, wrong_signature, right_pkey)
        assert False
    except InvalidSignature:
        pass

def test_crypt():
    right_key = generate_private_key()

    wrong_key = generate_private_key()

    right_pkey = right_key.public_key()

    wrong_pkey = wrong_key.public_key()

    message = b"A message I want to send"

    right_ciphertext = encrypt(message, right_pkey)

    wrong_ciphertext = encrypt(message, wrong_pkey)

    assert decrypt(right_ciphertext, right_key) == message

    try:
        decrypt(wrong_ciphertext, right_key)
        assert False
    except ValueError as e:
        assert str(e) == "Decryption failed."


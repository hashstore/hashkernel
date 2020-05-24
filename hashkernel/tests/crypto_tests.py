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
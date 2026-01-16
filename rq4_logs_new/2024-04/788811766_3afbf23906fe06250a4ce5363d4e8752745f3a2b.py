from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import base64
import time

def generate_ecc_key_pair(curve):
    private_key = ec.generate_private_key(curve, default_backend())
    public_key = private_key.public_key()
    return private_key, public_key

def perform_diffie_hellman(private_key, peer_public_key):
    shared_key = private_key.exchange(ec.ECDH(), peer_public_key)
    return shared_key

def derive_symmetric_key(shared_key):
    derived_key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=b'symmetric key',
        backend=default_backend()
    ).derive(shared_key)
    return derived_key

def encrypt_data(plaintext, symmetric_key):
    iv = b'\x00' * 16
    cipher = Cipher(algorithms.AES(symmetric_key), modes.CFB(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(plaintext) + encryptor.finalize()
    return ciphertext

def decrypt_data(ciphertext, symmetric_key):
    iv = b'\x00' * 16
    cipher = Cipher(algorithms.AES(symmetric_key), modes.CFB(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    plaintext = decryptor.update(ciphertext) + decryptor.finalize()
    return plaintext

# Step 1: Key Generation
start_time = time.time()
private_key_A, public_key_A = generate_ecc_key_pair(ec.SECP256R1())
end_time = time.time()
elapsed_time = (end_time - start_time) * 1000
print("Key Generation time A(256 bits):",elapsed_time)

start_time = time.time()
private_key_B, public_key_B = generate_ecc_key_pair(ec.SECP256R1())
end_time = time.time()
elapsed_time = (end_time - start_time) * 1000
print("Key Generation time B(256 bits):",elapsed_time)

# Display Keys with Key Size in Bits
print("Private Key A ({} bits):".format(private_key_A.curve.key_size), private_key_A.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption()
).decode('utf-8'))

print("Public Key A ({} bits):".format(public_key_A.curve.key_size), public_key_A.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
).decode('utf-8'))

print("Private Key B ({} bits):".format(private_key_B.curve.key_size), private_key_B.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption()
).decode('utf-8'))

print("Public Key B ({} bits):".format(public_key_B.curve.key_size), public_key_B.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
).decode('utf-8'))

# Step 2: Key Exchange
shared_key_A = perform_diffie_hellman(private_key_A, public_key_B)
shared_key_B = perform_diffie_hellman(private_key_B, public_key_A)

# Step 3: Key Derivation
symmetric_key_A = derive_symmetric_key(shared_key_A)
symmetric_key_B = derive_symmetric_key(shared_key_B)

# Display Shared Key
print("Shared Key A (hex):", shared_key_A.hex())
print("Shared Key B (hex):", shared_key_B.hex())

# Encryption
plaintext = input("Enter plain text: ").encode('utf-8')
print()
enc_start_time = time.time()*1000
ciphertext = encrypt_data(plaintext, symmetric_key_A)
print("Encrypted data:", base64.b64encode(ciphertext).decode('utf-8'))
enc_end_time=time.time()*1000
enc_elapsed_time=enc_end_time-enc_start_time
print("Encryption time:",enc_elapsed_time)
print()

# Decryption
dec_start_time=time.time()*1000
decrypted_text = decrypt_data(ciphertext, symmetric_key_B).decode('utf-8')
print("Decrypted data:", decrypted_text)
dec_end_time=time.time()*1000
dec_elapsed_time=(dec_end_time)-(dec_start_time)
print("Decryption time:",dec_elapsed_time)
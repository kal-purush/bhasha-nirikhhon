
import yaml
import maskpass
from globals import EMAIL_PATH, PASS_PATH, SECRET_PATH, AUTH_PATH

# Using pycroptodome. See https://nitratine.net/blog/post/python-encryption-and-decryption-with-pycryptodome
from Cryptodome.Random import get_random_bytes
from Cryptodome.Protocol.KDF import PBKDF2
from Cryptodome.Cipher import AES
from Cryptodome.Util.Padding import pad, unpad
 

def generate_key():
    return get_random_bytes(32) # 32 bytes * 8 = 256 bits (1 byte = 8 bits)


def store_key(key, path):
    with open(path, "wb") as file_out: # wb = write bytes
        file_out.write(key)


def encrypt(key, message, output_path): 
    data = message.encode('utf-8')

    # Create cipher object and encrypt the data
    cipher = AES.new(key, AES.MODE_CBC) # Create a AES cipher object with the key using the mode CBC
    ciphered_data = cipher.encrypt(pad(data, AES.block_size)) # Pad the input data and then encrypt

    with open(output_path, "wb") as file: 
        file.write(cipher.iv) # Write the iv to the output file (will be required for decryption)
        file.write(ciphered_data) # Write the varying length ciphertext to the file (this is the encrypted data)
    
    return output_path

    
def decrypt(key, input_file):
    ciphered_data = None
    with open(input_file, 'rb') as file: # Open the file to read bytes
        iv = file.read(16) # Read the iv out - this is 16 bytes long
        ciphered_data = file.read() # Read the rest of the data

    cipher = AES.new(key, AES.MODE_CBC, iv=iv)  # Setup cipher
    deciphered_bytes = unpad(cipher.decrypt(ciphered_data), AES.block_size) # Decrypt and then up-pad the result
    return deciphered_bytes.decode('utf-8')


def save_config(conf, path) -> None:
    with open(path, 'w') as file:
        yaml.safe_dump(conf, file)
    


if __name__ == "__main__":
    print("Enter Google Credentials. Password will be masked")
    email = input("email: ")
    password = maskpass.askpass("password: ")

    # Generate a salt and password
    salt = generate_key()
    key = PBKDF2(password, salt, dkLen=32)

    auth = dict()
    auth["email"] = encrypt(key, email, EMAIL_PATH)
    auth["password"] = encrypt(key, password, PASS_PATH)
    save_config(auth, AUTH_PATH)
    store_key(key, SECRET_PATH)
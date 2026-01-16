from cryptography.fernet import Fernet

with open("key.key", "rb") as key_file:
    key = key_file.read()

fernet = Fernet(key)

with open("keylog.enc", "rb") as log_file:
    for line in log_file:
        decrypted = fernet.decrypt(line.strip())
        print(decrypted.decode())
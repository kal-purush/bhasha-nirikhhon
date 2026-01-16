import hashlib
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15
from cryptography.hazmat.primitives import hashes
import itertools
import random
import string
import bcrypt
import time

# Global variables for encryption
aes_key = Fernet.generate_key()
aes_cipher = Fernet(aes_key)
private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
public_key = private_key.public_key()

def loading_animation(text="Processing"):
    for _ in range(3):
        print(f"{text}.", end="", flush=True)
        time.sleep(0.5)
    print(" Done!")

# Menu Functions
def hash_password():
    print("\n🔒 Welcome to the Password Hashing Tool!")
    password = input("Enter a password to hash: ")
    print("\nGenerating hashes...")
    loading_animation()

    sha256_hashed = hashlib.sha256(password.encode()).hexdigest()
    print(f"\n✅ SHA-256 Hashed Password: {sha256_hashed}")

    # Use bcrypt directly
    salt = bcrypt.gensalt()
    bcrypt_hashed = bcrypt.hashpw(password.encode(), salt)
    print(f"✅ Bcrypt Hashed Password: {bcrypt_hashed.decode()}")

    verification = bcrypt.checkpw(password.encode(), bcrypt_hashed)
    print(f"🔍 Password Verification Result: {'Valid' if verification else 'Invalid'}")

def brute_force_crack():
    print("\n🛠️ Brute Force Attack Tool")
    target_hash = input("Enter the SHA-256 hash to crack: ")
    charset = input("Enter the character set (e.g., 'abc123'): ")
    max_length = int(input("Enter the maximum password length: "))

    print("\nStarting brute force attack...")
    loading_animation("Attempting passwords")

    for length in range(1, max_length + 1):
        print(f"Trying passwords of length: {length}")
        for attempt in itertools.product(charset, repeat=length):
            attempt_password = ''.join(attempt)
            attempt_hash = hashlib.sha256(attempt_password.encode()).hexdigest()
            if attempt_hash == target_hash:
                print(f"\n🎉 Password found: {attempt_password}")
                return
    print("\n❌ Password not found within the specified parameters.")

def dictionary_attack():
    print("\n📖 Dictionary Attack Tool")
    target_hash = input("Enter the SHA-256 hash to crack: ")
    wordlist_path = input("Enter the path to the wordlist file: ")

    print("\nScanning wordlist...")
    loading_animation()

    try:
        with open(wordlist_path, 'r') as file:
            for word in file:
                word = word.strip()
                attempt_hash = hashlib.sha256(word.encode()).hexdigest()
                if attempt_hash == target_hash:
                    print(f"\n🎉 Password found: {word}")
                    return
        print("\n❌ Password not found in the wordlist.")
    except FileNotFoundError:
        print("\n❌ Wordlist file not found. Please check the path and try again.")

def enforce_password_policy():
    print("\n🔐 Password Policy Enforcement Tool")
    def generate_password(length=12):
        characters = string.ascii_letters + string.digits + string.punctuation
        return ''.join(random.choice(characters) for _ in range(length))

    print("\nGenerating a strong password...")
    loading_animation()

    strong_password = generate_password()
    print(f"\n🔑 Generated Strong Password: {strong_password}")

def aes_encryption():
    print("\n🔒 AES Encryption Tool")
    password = input("Enter a password to encrypt: ")

    print("\nEncrypting your password...")
    loading_animation()

    encrypted = aes_cipher.encrypt(password.encode())
    print(f"\n🔐 AES Encrypted Password: {encrypted}")

    decrypted = aes_cipher.decrypt(encrypted).decode()
    print(f"🔓 AES Decrypted Password: {decrypted}")

def rsa_encryption():
    print("\n🔒 RSA Encryption Tool")
    password = input("Enter a password to encrypt: ")

    print("\nEncrypting your password using RSA...")
    loading_animation()

    encrypted = public_key.encrypt(password.encode(), PKCS1v15())
    print(f"\n🔐 RSA Encrypted Password: {encrypted}")

    decrypted = private_key.decrypt(encrypted, PKCS1v15()).decode()
    print(f"🔓 RSA Decrypted Password: {decrypted}")

# Main Menu
def main():
    while True:
        print("\n✨ Welcome to the Password Toolkit ✨")
        print("Choose an option below:")
        print("1️⃣  Hash a password")
        print("2️⃣  Brute force attack")
        print("3️⃣  Dictionary attack")
        print("4️⃣  Enforce password policy")
        print("5️⃣  AES encryption")
        print("6️⃣  RSA encryption")
        print("7️⃣  Exit")
        
        choice = input("Enter your choice: ")

        if choice == "1":
            hash_password()
        elif choice == "2":
            brute_force_crack()
        elif choice == "3":
            dictionary_attack()
        elif choice == "4":
            enforce_password_policy()
        elif choice == "5":
            aes_encryption()
        elif choice == "6":
            rsa_encryption()
        elif choice == "7":
            print("\n👋 Thank you for using the Password Toolkit. Goodbye!")
            break
        else:
            print("\n❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main()

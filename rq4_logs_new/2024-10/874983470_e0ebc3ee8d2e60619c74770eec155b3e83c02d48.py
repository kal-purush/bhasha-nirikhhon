import json
import os
from cryptography.fernet import Fernet

# Generar una clave de cifrado y guardarla en un archivo
def generate_key():
    key = Fernet.generate_key()
    with open("secret.key", "wb") as key_file:
        key_file.write(key)

# Cargar la clave de cifrado desde el archivo
def load_key():
    return open("secret.key", "rb").read()

# Cifrar una contraseña
def encrypt_password(password, key):
    f = Fernet(key)
    encrypted_password = f.encrypt(password.encode())
    return encrypted_password

# Descifrar una contraseña
def decrypt_password(encrypted_password, key):
    f = Fernet(key)
    decrypted_password = f.decrypt(encrypted_password).decode()
    return decrypted_password

# Guardar una contraseña cifrada en el archivo JSON
def save_password(service, username, password, key):
    encrypted_password = encrypt_password(password, key)
    if os.path.exists("passwords.json"):
        with open("passwords.json", "r") as file:
            passwords = json.load(file)
    else:
        passwords = {}
    
    passwords[service] = {"username": username, "password": encrypted_password.decode()}
    
    with open("passwords.json", "w") as file:
        json.dump(passwords, file, indent=4)

# Recuperar y descifrar una contraseña del archivo JSON
def get_password(service, key):
    with open("passwords.json", "r") as file:
        passwords = json.load(file)
    
    if service in passwords:
        encrypted_password = passwords[service]["password"].encode()
        username = passwords[service]["username"]
        password = decrypt_password(encrypted_password, key)
        return username, password
    else:
        return None

# Menú principal
def main():
    if not os.path.exists("secret.key"):
        generate_key()
    
    key = load_key()

    while True:
        print("\nAdministrador de Contraseñas")
        print("1. Guardar una nueva contraseña")
        print("2. Recuperar una contraseña")
        print("3. Salir")
        choice = input("Elige una opción: ")
        
        if choice == "1":
            service = input("Nombre del servicio: ")
            username = input("Nombre de usuario: ")
            password = input("Contraseña: ")
            save_password(service, username, password, key)
            print("Contraseña guardada exitosamente.")
        
        elif choice == "2":
            service = input("Nombre del servicio: ")
            result = get_password(service, key)
            if result:
                username, password = result
                print(f"Nombre de usuario: {username}")
                print(f"Contraseña: {password}")
            else:
                print("Servicio no encontrado.")
        
        elif choice == "3":
            break
        
        else:
            print("Opción no válida. Inténtalo de nuevo.")

if __name__ == "__main__":
    main()
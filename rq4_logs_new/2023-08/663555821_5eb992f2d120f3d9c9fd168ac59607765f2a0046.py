import socket

def encrypt_text(plaintext, key):
    encrypted_text = ""
    for char in plaintext:
        if char.isalpha():
            encrypted_char = chr((ord(char) + key - ord('A')) % 26 + ord('A'))
        else:
            encrypted_char = char
        encrypted_text += encrypted_char
    return encrypted_text

def main():
    host = input("Enter the server IP: ")
    port = int(input("Enter the server port: "))
    message = input("Enter the message to send: ")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))

        # Concatenate encrypted and plain text with a delimiter (e.g., ";")
        encrypted_message = "ENCRYPTED:" + encrypt_text(message, 3) + ";" + message
        client_socket.sendall(encrypted_message.encode())

if __name__ == "__main__":
    main()
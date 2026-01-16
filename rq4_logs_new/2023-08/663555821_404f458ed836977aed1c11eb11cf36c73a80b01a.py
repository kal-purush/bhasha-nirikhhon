import socket
import math
import json
import sys

def calculate_entropy(data):
    if not data:
        return 0

    entropy = 0
    for char in set(data):
        prob = float(data.count(char)) / len(data)
        entropy -= prob * math.log2(prob)

    return entropy

server = sys.argv[1]
port = sys.argv[2]

print(f"Listening for incoming messages on {server}:{port}...")
irc_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

irc_socket.bind((server, port))

irc_socket.settimeout(60)

output_data = []

try:
    irc_socket.listen(1)

    client_socket, client_address = irc_socket.accept()
    print(f"Connected to {client_address}")

    while True:
        data = client_socket.recv(2048).decode()

        if not data:
            break

        print(f"Received message: {data}")

        entropy = calculate_entropy(data)
        print(f"Entropy of received data: {entropy:.2f}")

        if entropy > 3.0:
            print("The received data might be encrypted.")
            is_encrypted = True
        else:
            print("The received data might not be encrypted.")
            is_encrypted = False

        message_details = {
            "message": data,
            "entropy": entropy,
            "is_encrypted": is_encrypted
        }
        output_data.append(message_details)

    client_socket.close()

except socket.timeout:
    print("Socket timeout. No incoming connections received.")
finally:
    irc_socket.close()

output_filename = "received_messages.json"
with open(output_filename, "w") as json_file:
    json.dump(output_data, json_file, indent=4)

print(f"Output written to {output_filename}")
import socket
import math
import json

def calculate_entropy(data):
    if not data:
        return 0

    entropy = 0
    for char in set(data):
        prob = float(data.count(char)) / len(data)
        entropy -= prob * math.log2(prob)

    return entropy

# Get IRC server information from the user
server = input("Enter IRC server IP address: ")
port = int(input("Enter IRC server port: "))

print(f"Listening for incoming messages on {server}:{port}...")
# Create a socket
irc_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the specified IP address and port
irc_socket.bind((server, port))

# Set a timeout of 2 minutes (120 seconds) for the socket
irc_socket.settimeout(60)

output_data = []

try:
    # Listen for incoming connections
    irc_socket.listen(1)

    # Accept a connection
    client_socket, client_address = irc_socket.accept()
    print(f"Connected to {client_address}")

    while True:
        data = client_socket.recv(2048).decode()

        if not data:
            break

        print(f"Received message: {data}")

        # Calculate entropy of received data
        entropy = calculate_entropy(data)
        print(f"Entropy of received data: {entropy:.2f}")

        # Basic check for encrypted data based on entropy
        if entropy > 3.0:
            print("The received data might be encrypted.")
            is_encrypted = True
        else:
            print("The received data might not be encrypted.")
            is_encrypted = False

        # Prepare data for JSON
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

# Write output to JSON file
output_filename = "received_messages.json"
with open(output_filename, "w") as json_file:
    json.dump(output_data, json_file, indent=4)

print(f"Output written to {output_filename}")
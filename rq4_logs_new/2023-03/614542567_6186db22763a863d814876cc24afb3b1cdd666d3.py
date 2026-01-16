# Create a client that can send messages to the server and receive replies running on port 8000 from the server in server.py

import socket
import sys
# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


# Get the IP address from command line arguments
if len(sys.argv) != 2:
    print("Usage: python client.py <IP address>")
    sys.exit(1)

# Connect the socket to the port where the server is listening
IP_ADDRESS = sys.argv[1]
server_address = (IP_ADDRESS, 8000)


print('connecting to %s' % server_address[0])
sock.connect(server_address)

try:

    message = ""
    while message.lower().strip() != 'quit':
        message = input(" -> ")  # take input
        sock.send(message.encode())  # send message
        data = sock.recv(1024).decode()  # receive response

        if not data:
            # if data is not received break
            break

        print(data)  # show in terminal


finally:
    print('closing socket')
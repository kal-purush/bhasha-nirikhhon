import socket
import sys

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect the socket to the port where the server is listening
server_address = ('localhost', 10000)
print('Conectando al {} puerto {}'.format(*server_address))
sock.connect(server_address)

try:

    # Send data
    message = b'Este es el mensaje.'
    print('Enviando {!r}'.format(message))
    sock.sendall(message)

    # Look for the response
    amount_received = 0
    amount_expected = len(message)

    while amount_received < amount_expected:
        data = sock.recv(32)
        amount_received += len(data)
        print('Recibido {!r}'.format(data))

finally:
    print('Cerrando socket')
    sock.close()
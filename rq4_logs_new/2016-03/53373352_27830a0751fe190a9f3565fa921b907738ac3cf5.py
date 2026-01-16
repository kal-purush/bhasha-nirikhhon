# -*- coding: utf-8 -*-

import socket

server_socket = socket.socket(socket.AF_INET,
                    socket.SOCK_STREAM,
                    socket.IPPROTO_TCP,)
print("\nserver: ", server_socket)


address = ('127.0.0.1', 5000)
server_socket.bind(address)
print("\nserver: ", server_socket)


server_socket.listen(1)
print("\nlistening...")
conn, addr = server_socket.accept()


# Receive the buffered message
buffered_message = ""
buffer_length = 4096
message_complete = False
while not message_complete:
    part = conn.recv(buffer_length)
    buffered_message += part.decode('utf8')
    if len(part) < buffer_length:
        break




# TODO: When the Client hid Ctrl+D then the connection closes

conn.close()  # close the CONNECTION after reading.
print("\nclosed: connection")

server_socket.close() # closes the SERVER SOCKET when everything is done
print("\nclosed: server")
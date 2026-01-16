import sample_payload_pb2
import random
import string
import socket
import sys
import ssl


def random_string_generator(size=6, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def random_int_generator():
    return random.randint(0, 999999)

BUFSIZ = 1024 # bufsize = max receivable at once

SERVER_ADDRESS = ('localhost', 9999)
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
wrapped_client_socket = ssl.wrap_socket(client_socket, ssl_version=ssl.PROTOCOL_TLSv1, ciphers="ADH-AES256-SHA")

messages = [] # empty list to store the messages
names = ["jack", "jill", "alex", "agnes"]
for n in names:
    payload = sample_payload_pb2.SamplePayload()
    payload.userName = n
    payload.userAge = 100
    payload.randomString = random_string_generator()
    payload.randomInt = random_int_generator()
    messages.append(payload)

reply = ''

wrapped_client_socket.connect(SERVER_ADDRESS)

for s in messages:

    print("Sending: " + str(s))
    print("Raw message: " + s.SerializeToString())
    wrapped_client_socket.send(s.SerializeToString()) # serialize our message to string
    reply = wrapped_client_socket.recv(BUFSIZ)

    if not reply:
        break

    print(reply + " from: " + str(SERVER_ADDRESS))

wrapped_client_socket.close()
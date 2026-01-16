import socket
import cv2
import numpy as np
import time
import sys
import argparse
parser = argparse.ArgumentParser(description='use your raspberry pi to make a security camera')
parser.add_argument('--port', nargs='?', const=1234, type=int, help='the port used on the raspberry pi')
parser.add_argument('--ip', type=str, help='the ip of raspberry pi')
args = parser.parse_args()
port=args.port
ip=args.ip
#trying to connect to server
try:
    s = socket.socket()
    s.connect((ip,port))
except OSError:
    print('ip or port not founded')
    quit(1)
print('connected')
#making sure the server is ready
while True:
    s.send(b'ready')
    x = s.recv(1024)
    if x == b'start':
        break
while True:
    al = b''
    #making sure the server is on the same pace
    while True:
        x = s.recv(1024)
        if x == b'start':
            break
    #start receiving
    while True:
        x = s.recv(4096)
        if x != b'':
            if b'done' in x:
                x.replace(b'done', b'')
                al+=x
                break
            al+=x
        else:
            break
    #start decoding
    try:
        image = np.asarray(bytearray(al), dtype="uint8")
        image = cv2.cvtColor(cv2.imdecode(image, cv2.IMREAD_COLOR), cv2.COLOR_BGR2RGB)
        cv2.imshow('image', image)
        key = cv2.waitKey(1)
        if key == ord('q'):
            break
    except:
        pass
    time.sleep(0.1)
s.close()
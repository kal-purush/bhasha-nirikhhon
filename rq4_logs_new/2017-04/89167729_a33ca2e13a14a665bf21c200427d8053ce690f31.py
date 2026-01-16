import os
import pickle
import threading
from socket import *
host = "192.168.43.148" # set to IP address of target computer
port = 13000
buf = 1024
addr = (host, port)
UDPSock = socket(AF_INET, SOCK_DGRAM)
ext = ['.txt']
all_files_content = {}
def perform_sync():
    threading.Timer(10.0, perform_sync).start()
    print(os.getcwd())
    files_all = [f for f in os.listdir(os.getcwd()) if f.endswith(tuple(ext))]
    print(files_all)
    for file_name in files_all:
        with open (file_name, 'r+') as f:
            file_b_content = f.readlines()
            f.close()
        with open (file_name, 'w+') as f:
            f.close()
        all_files_content[file_name] = file_b_content
    print(all_files_content)
    hh = pickle.dumps(all_files_content)
    print(hh)
    UDPSock.sendto(hh,addr)
while True:
    data = input("Enter message to send or type 'exit': ")
    if data == "sync":
        perform_sync()
    (data, addr) = UDPSock.recvfrom(1024)
    updated = pickle.loads(data)
    for k, v in updated.items():
        with open (k, 'w+') as f:
            for s in v:
                f.write(s)
    if data == "exit":
        break
UDPSock.close()
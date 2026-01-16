import socket
import threading

def send_request(data):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('example.com', 80))
    sock.sendall(data.encode())
    sock.close()

# change num as desired. Might lead to performance overhead and security concerns
num = 1000 
threads = []
for i in range(num):
    data = "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"
    thread = threading.Thread(target=send_request, args=(data,))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()
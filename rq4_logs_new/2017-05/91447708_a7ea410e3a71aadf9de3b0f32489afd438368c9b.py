import socket
import time
import sys

#sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#sock.connect(('202.89.233.103', 80))
#
#(ipv4, port) = sock.getsockname()
#
#print str((ipv4, port))
#
#
#while True:
#    sock.sendall("GET / HTTP/1.1\r\nHost: cn.bing.com\r\n\r\n")
#
#    reply = sock.recv(1024*1024)
#    print reply
#
#    time.sleep(6)
#    sys.exit(0)



UDP_IP = "202.97.144.47"
UDP_PORT = 5555
MESSAGE = "Hello, World!"

print "UDP target IP:", UDP_IP
print "UDP target port:", UDP_PORT
print "message:", MESSAGE

sock = socket.socket(socket.AF_INET, # Internet
             socket.SOCK_DGRAM) # UDP
sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))
print "Local IP:", sock.getsockname()[0]
print "Local Port:", sock.getsockname()[1]
while True:
    time.sleep(1)

from urllib2 import urlopen as request
import thread
import httplib
import ssl

url = "http://www.baidu.com/"
https_url = "https://tieba.baidu.com/index.html"
def http_request(i):
    response = request(https_url)
    print str(response.getcode())
    print str(response.read())
   
def start_threads():
    for i in range(0, 1):
        thread.start_new_thread(http_request, (i,))

start_threads()

while True:
    time.sleep(1)
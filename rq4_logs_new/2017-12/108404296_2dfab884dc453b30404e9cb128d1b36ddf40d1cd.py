import socket
import select
import sys

soc_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
if len(sys.argv) != 3:
    print "Correct usage: script, IP address, port number"
    exit()
IP_address = str(sys.argv[1])
Port = int(sys.argv[2])
soc_server.connect((IP_address, Port))

while True:
    sockets_list = [sys.stdin, soc_server]
    read_sockets,write_socket, error_socket = select.select(sockets_list, [], [])
    for socks in read_sockets:
        if socks == soc_server:
            message = socks.recv(2048)
            print message
        else:
            message = sys.stdin.readline()
            soc_server.send(message)
            sys.stdout.write("<You>")
            sys.stdout.write(message)
            sys.stdout.flush()
soc_server.close()
import socket

s=socket.socket()
host = '127.0.0.1'
port = 5000
s.bind((host,port))
s.listen(5)
print 'Connecting ...'
while True:
    # establish a connection
    clientsocket,addr = s.accept()      

    print("Got a connection from %s" % str(addr))
    try:
    	clientsocket.send('Connection Established')
    	data = raw_input(">>")
    	while data!= 'q':
    		clientsocket.send(data)
    		data = raw_input(">>")
    	#print s.recv(1024)
    	clientsocket.close()
    except:
    	clientsocket.close()
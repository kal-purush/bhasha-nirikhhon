import socket
import func

choice=raw_input("Connect to a person or start a server 1|2 :\n")
if choice == '1':
	s=socket.socket()
	host = func.return_ip()
	port = int(raw_input("Enter the port\n"))
	print ("Server IP | Port are %s | %d" %(host,port))
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
			while True:
				clientsocket.send(data)
				data = raw_input(">>")
				if data == 'q':
					clientsocket.close()	
    		    #print s.recv(1024)
    		
		except:
			clientsocket.close()
if choice == '2':
	s= socket.socket()
	host = raw_input("Enter the IP to connect to : ")
	port = int(raw_input("Enter the port: "))
	s.connect((host, port))

	data = s.recv(1024)
	s.sendall(raw_input(""))
	while True:
		data = s.recv(1024)
		if data:
			print host + ">>"+data;
	s.close()
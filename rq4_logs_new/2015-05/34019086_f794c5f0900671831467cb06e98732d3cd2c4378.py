import socket
import threading
import time
#rendezvous  server for UDP hole punching
server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.bind(('0.0.0.0', 5006))
need_to_disconnect_inactive_clients = False


clients_adresses = set()
ishere = set()

def disconnectInactiveClients():
    global clients_adresses
    global ishere
    global need_to_disconnect_inactive_clients
    while 1 == 2:
        if not need_to_disconnect_inactive_clients:
            time.sleep(3.0)
            continue
        need_to_disconnect_inactive_clients = False
        for client_address in clients_adresses:   
            print 'asked if %s is here' % str(client_address)
            server_socket.sendto('Are you here?',client_address)
            print 'went to sleep for 1.5 seconds'
            time.sleep(1.5)  
            print 'woke up after a 1.5 seconds sleep'
            if client_address not in ishere:                
                print 'disconnecting ' + str(client_address)
                clients_adresses.discard(client_address)
                server_socket.sendto('YOU ARE DISCONNECTED', client_address)
            else:
                ishere.discard(client_address)
        print clients_adresses
def prezendousServer():
    global server_socket
    global clients_adresses
    global ishere
    global need_to_disconnect_inactive_clients
    got_data_already = False
    while True:
        if not got_data_already:
            try:
                (data, client_address) = server_socket.recvfrom(1024)
            except Exception as e:
                print 'faced an exception', e
                #server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                #server_socket.bind(('0.0.0.0', 5006))            
                continue
        got_data_already = False
        ishere.add(client_address)
        clients_adresses.add(client_address)
        print clients_adresses
        if(data == 'Get me an opponent'):
            totaldata = ''
            print 'sure dude, i will find you an opponent. wait a second'
            if len(clients_adresses) < 2:
                print 'not enough clients connected yet. wait for another client to connect'
                continue
            clients_to_remove = set()
            for address in clients_adresses:
                if address != client_address:
                    print 'asked %s if he wants to connect.' % str(address)
                    server_socket.sendto('Want to connect with someone?' + '\x00', address)
                    clients_to_remove.add(address)    
            print 'waiting for a response from one of the other clients.'
            server_socket.settimeout(0.5)
            data = None  
            timeouts = 0            
            try:
                print 'trying to recvfrom a client'
                data, address = server_socket.recvfrom(1024)
            except socket.timeout:
                timeouts += 1
                print 'timed out.'
                if timeouts == len(clients_adresses)+1:  #because clients_adresses contains the clients socket that shouldn't answer as well.
                    break
            except socket.error as e:  
                print 'socket error', e
                try:
                    data, address = server_socket.recvfrom(1024)
                except:
                    pass
            print 'I got: ', data
            server_socket.settimeout(None)
            print 'finished trying to recieve from sockets.'
            if data == 'Yes, I do want to connect':
                for client in clients_to_remove:
                    if client != address and client != client_address:
                        server_socket.sendto('I removed you' + '\x00', client)
                        clients_adresses.remove(client)
                server_socket.sendto('Get ready for IP.' + '\x00', client_address)
                print 'one of the clients does want to connect with you.', address, 'he sent: ', data                
                ishere.add(address)         
                print 'got answer from ', str(address) + '\nconnecting him with requester. created holes in NAT.'
                server_socket.sendto(str(address[0]) + '\x00', client_address)
                time.sleep(0.05)
                server_socket.sendto(str(address[1]) + '\x00', client_address) 
                time.sleep(0.05)
                server_socket.sendto(str(client_address[0]) + '\x00', address)
                time.sleep(0.05)
                server_socket.sendto(str(client_address[1]) + '\x00', address)                
                print '\n\n client adress:', str(client_address[0]) ,',', str(client_address[1]) , ' address: ',str(address[0]) ,',', str(address[1]) ,'\n\n'
                clients_adresses.discard(client_address)
                if address in clients_adresses:
                    clients_adresses.discard(address)
                need_to_disconnect_inactive_clients = True
            elif data == 'Get me an opponent':
                got_data_already = True
                continue
        elif data == 'Remove me from waiting list.':
            print data
            print 'Ok, I will remove you from the waiting list'
            if client_address in clients_adresses:
                clients_adresses.discard(client_address)
           

threads = []

thread_disconnectInactiveClients = threading.Thread(name='disconnectInactiveClients', target=disconnectInactiveClients)
thread_disconnectInactiveClients.start()
prezendousServer()
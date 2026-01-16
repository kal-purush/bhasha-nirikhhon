import socket
import sys
import threading
import os
import random
import time
import pickle as p

# start servers/client, client1 send operation, server1 failprocess, client1 send operation, server1 reconnect, client1 send operation, client2 send operation = not getting result from server1

global SERVER_PORTS, SERVER_SOCKETS, SERVER_SOCKET, LEADER_HINT, RECEIVED
SERVER_PORTS = {
    1: 5001, 
    2: 5002,
    3: 5003,
    4: 5004,
    5: 5005
}
CLIENT_PORTS = {
    1: 5006, 
    2: 5007
}
CLIENT_ID = 0
SERVER_NUMS = [1,2,3,4,5]
SERVER_SOCKETS = {}
SERVER_SOCKET = None
LEADER_HINT = None
RECEIVED = False
FAULTY_LEADERS = []

def close_connection():
    global SERVER_SOCKET
    SERVER_SOCKET.close()
    os._exit(0)

def time_out(duration, line): 
    global RECEIVED, LEADER_HINT, FAULTY_LEADERS
    time.sleep(duration)
    if RECEIVED == False: 
        print("not received")
        # random server that is not the failed leader (LEADER_HINT)
        FAULTY_LEADERS.append(LEADER_HINT)
        print(FAULTY_LEADERS)
        if len(FAULTY_LEADERS) >= 3:
            print("majority of leaders faulty")
        else:
            while LEADER_HINT in FAULTY_LEADERS: 
                LEADER_HINT = random.randint(1,5)
            # resend operation
            RECEIVED = False
            print("resending operation")
            if SERVER_SOCKETS[LEADER_HINT] != None:
                SERVER_SOCKET = SERVER_SOCKETS[LEADER_HINT]
                SERVER_SOCKET.sendall(p.dumps(("Operation", line, CLIENT_ID)))
                timeout_thread = threading.Thread(target=time_out, args=(30.0, line))
                timeout_thread.start()
                timeout_thread.join()
            else: 
                print("in timeout, server_sockets[{}] was none".format(LEADER_HINT))

# handle inputs
def handle_inputs(): 
    global SERVER_SOCKETS, LEADER_HINT, FAULTY_LEADERS
    while True: 
        try: 
            line = input()
            line_split = line.split(" ")
            if (line == 'e'):
                # do_exit(SERVER_SOCKETS[0], SERVER_SOCKETS[1], SERVER_SOCKETS[2], SERVER_SOCKETS[3], SERVER_SOCKETS[4])
                close_connection()
            elif "Operation" in line:
                if LEADER_HINT is None:
                    LEADER_HINT = random.randint(1,5)
                    if len(FAULTY_LEADERS) >= 3:
                        print("majority of leaders faulty")
                    else:
                        while LEADER_HINT in FAULTY_LEADERS: 
                            LEADER_HINT = random.randint(1,5)
                        if int(CLIENT_ID) == 1:
                            LEADER_HINT = 3
                        else: 
                            LEADER_HINT = 3
                        print("sending to {}".format(LEADER_HINT))
                        SERVER_SOCKET = SERVER_SOCKETS[LEADER_HINT]
                        RECEIVED = False
                        FAULTY_LEADERS = []
                        SERVER_SOCKET.sendall(p.dumps(("Operation", line, CLIENT_ID)))
                        # thread that sleeps for 5 seconds
                        threading.Thread(target=time_out, args=(30.0, line)).start()
                else:
                    #send to server
                    print("sending to {}".format(LEADER_HINT))
                    SERVER_SOCKET = SERVER_SOCKETS[LEADER_HINT]
                    RECEIVED = False
                    FAULTY_LEADERS = []
                    SERVER_SOCKET.sendall(p.dumps(("Operation", line, CLIENT_ID)))
                    threading.Thread(target=time_out, args=(30.0, line)).start()
            elif "reconnect" in line:
                server = int(line[-1])
                print("reconnecting to server {}".format(server))
                SERVER_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                SERVER_SOCKET.connect((socket.gethostname(), SERVER_PORTS[server]))
                SERVER_SOCKET.sendall(p.dumps(("client", CLIENT_ID)))
                SERVER_SOCKETS[server-1] = SERVER_SOCKET
        except EOFError:
            pass

def recv_server(server):
    global SERVER_SOCKETS, RECEIVED, LEADER_HINT
    while True:
        if SERVER_SOCKETS[server] != None:
            print("checking word for server {}".format(server))
            word = SERVER_SOCKETS[server].recv(4096).decode()
            if word != "" and 'failProcess' not in word:
                RECEIVED = True
                print("{}: {}".format(server, word))
                if word[2] != "" and "DOES NOT EXIST" not in word and int(word[2]) != LEADER_HINT and int(word[2]) != 0:
                    print("setting LEADER_HINT to {}".format(int(word[2])))
                    LEADER_HINT = int(word[2])
            elif 'failProcess' in word:
                print("in failProcess")
                # server id, subtract 1 to get array index from SERVER_SOCKETS
                server_index = int(word[-1])
                SERVER_SOCKETS[server_index].close()
                SERVER_SOCKETS[server_index] = None
                FAULTY_LEADERS.append(server_index)
                if LEADER_HINT == server_index:
                    LEADER_HINT = None

def handle_recv():
    # server listening for msgs
    global SERVER_SOCKETS, LEADER_HINT, RECEIVED
    try: 
        threading.Thread(target=recv_server, args=(1,)).start()
        threading.Thread(target=recv_server, args=(2,)).start()
        threading.Thread(target=recv_server, args=(3,)).start()
        threading.Thread(target=recv_server, args=(4,)).start()
        threading.Thread(target=recv_server, args=(5,)).start()
    except (socket.timeout, KeyboardInterrupt) as error:
        print(error)
        if error != "timed out":
            close_connection()

if __name__ == '__main__':
    CLIENT_ID = sys.argv[1]

    for num in SERVER_NUMS:
        SERVER_SOCKET = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        SERVER_SOCKET.connect((socket.gethostname(), SERVER_PORTS[num]))
        SERVER_SOCKET.sendall(p.dumps(("client", CLIENT_ID)))
        SERVER_SOCKETS[num] = SERVER_SOCKET

    threading.Thread(target=handle_inputs, args=()).start()
    handle_recv()
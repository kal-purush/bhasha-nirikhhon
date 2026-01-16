'''
Ganeshbabu Thavasimuthu
1001452475
'''

import socket
import sys
import json
import random 
from threading import Thread
from packet import Packet
import datetime
oldsysout = sys.stdout
    
class Airforce:    
    def __init__(self, ip, port):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.client_socket.bind((ip,3001))
        self.client_socket.connect((ip, port))
    
    def log(self, msg):
        log_file = open("airforce.log","a")
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H:%M")
        sys.stdout = log_file
        print(timestamp + ": "+ str(msg))
        sys.stdout = oldsysout
        print(msg)
        
    def establish_handshake(self):
        sourceid = 3001
        destid = 1234
        
        # Frame 1
        seqnum = random.randint(1000,2000)
        acknum = 0
        flags = (0,0,0,0,0,1,0)
        frame1 = Packet(sourceid, destid, seqnum, acknum, flags, "")
        self.client_socket.send(frame1.serialize().encode())
        self.log("\nResponse from server \n")
        while True:
            #self.log("waiting for response")
            response = self.client_socket.recv(1024).decode('utf-8')
            if not response:
                continue;
            tcppacket = json.loads(response)
            logpacket = " ** TCP PACKET ** \n"+" Source Port: {} \n Destination Port: {} \n Sequence Number: {} \n Ack Number: {}\n flags {} \n Data: {}".format(tcppacket['sourceid'], tcppacket['destid'],tcppacket['seqnum'],tcppacket['acknum'],tcppacket['flags'],tcppacket['data'])
            self.log(logpacket)
            #self.log("{}{}{}".format(tcppacket['sourceid'],tcppacket['flags'][4],tcppacket['flags'][-2]))
            
            # Frame 2
            if tcppacket['sourceid']==1234 and tcppacket['flags'][3]==1 and tcppacket['flags'][-2]==1:
                seqnum = tcppacket['acknum']
                acknum = tcppacket['seqnum'] + 1
                flags = (0,0,0,1,0,0,0)
                frame3 = Packet(sourceid, tcppacket['sourceid'], seqnum, acknum, flags, "")
                self.log("Sending frame3 to server")
                self.client_socket.send(frame3.serialize().encode())
                self.log("MissionTCP: Airforce, You are connected. Start the mission!!")
                break
            if not response:
                break
            self.log("\n")
        self.log("DONE")
        
                
airforce = Airforce(str(sys.argv[1]),int(sys.argv[2]))
airforce.log("*** HOST : Airforce *** ")
airforce.establish_handshake()

# Wait for the packet from Jan
while True:
    response = airforce.client_socket.recv(10024).decode('utf-8')   
    #print("response came")
    if response:
        tcppacket = json.loads(response)
        logpacket = " ** TCP PACKET ** \n"+" Source Port: {} \n Destination Port: {} \n Sequence Number: {} \n Ack Number: {}\n flags {} \n Data: {}".format(tcppacket['sourceid'], tcppacket['destid'],tcppacket['seqnum'],tcppacket['acknum'],tcppacket['flags'],tcppacket['data'])
        airforce.log(logpacket)
        if tcppacket['sourceid']==2002:
            airforce.log("Code received: "+tcppacket['data'])
            airforce.log("Target destroyed!")
            airforce.log("Sending success message to Jan")
            seqnum = tcppacket['acknum']
            acknum = tcppacket['seqnum'] + 1
            flags = (0,0,0,1,0,0,0)
            frame3 = Packet(3001, 2002, seqnum, acknum, flags, "MISSION SUCCESS")
            airforce.client_socket.send(frame3.serialize().encode())
            airforce.log("Disconnecting from MissionTCP")
            break
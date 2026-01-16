from socket import *
import sys, select
from RTPSocket import RTPSocket
from RTPSocketManager import RTPSocketManager
from RTPPacket import RTPPacket

#Check arguments length is valid
if len(sys.argv) < 4:
    print "Please use valid number of arguments"
    sys.exit()

#Parse paramteres
try:
    params = ""
    i = 0
    for arg in sys.argv:
        i = i + 1
        if i == 2:
            addressArray = arg.split(":")
            if len(addressArray) < 2:
                print "Please use valid address arguments"
                sys.exit()
            serverIpAddress = addressArray[0]
            serverPortNumber = int(addressArray[1])
        if i == 3:
            GTID = str(arg)
        if i == 4:
            params = str(arg)
        if i > 4:
            params = params + ", " + str(arg)
except Exception as e:
    print "Failure parsing parameters: " + str(e)
    sys.exit()

#socket() bind()
socketManager = RTPSocketManager()
socketManager.bindUDP(str(gethostbyname(getfqdn())), 8591)
socket = socketManager.createSocket(serverPortNumber)

serverQuery = GTID + ":" + params
socket.sendData(serverIpAddress, serverPortNumber, [serverQuery])
ipAddress, portNumber, dataArray, description = socket.recvData()

modifiedMessage = dataArray[0]

#print out result
print 'From Server:', modifiedMessage

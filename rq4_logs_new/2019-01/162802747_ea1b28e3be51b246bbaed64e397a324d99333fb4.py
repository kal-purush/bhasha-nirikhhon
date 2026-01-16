#
#   TestMode 014
#   Read GPS from Comm and print it
#
#

import time
import IMUGps
import Comm
import datetime

while True:
    
    msg = Comm.fnc_CommRecieve()
    print (msg)
    
    if msg[0:3] == "GPS":
        print ("Decrypting GPS Data...")
        msgSplit = msg.split(" ")
        time = msgSplit[2]
        lat = msgSplit[4]
        dirLat = msgSplit[5]
        lon = msgSplit[7]
        dirLon = msgSplit[8]
        
        print ("Position: Lat: " + lat + " " + dirLat + " Lon: " + lon + dirLon
    
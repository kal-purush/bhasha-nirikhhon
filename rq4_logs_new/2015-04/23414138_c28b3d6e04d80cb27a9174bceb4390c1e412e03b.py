#!/usr/bin/python
"""
Logs data from SparkFun SEN 11345 Geiger Counter
"""
import serial
import time
import json
import os
CONFFILE = open("CONFig.json", "r")
CONF = json.load(CONFFILE)
CONFFILE.close()
SP = serial.Serial(CONF['usbDev'], CONF['serialSpeed'])
OPENSUCCESS = False
FILENUMBER = 0
while OPENSUCCESS == False:
    try:
        FD = os.open("radlog-" + str(FILENUMBER) + ".dat",
        os.O_RDWR|os.O_CREAT|os.O_EXCL)
    except OSError:
        FILENUMBER += 1
    else:
        OPENSUCCESS = True
while True:
    #read will block until there is data
    SP.read()
    os.write(FD, str(int(time.time())) + "\n")
    #flush and sync
    os.fsync(FD)
os.close(FD)
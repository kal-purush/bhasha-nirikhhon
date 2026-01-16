#!/usr/bin/python

from __future__ import division
import time
import os

print "Starting..."

while True:
    #-----STORAGE-----
    rawCommand = os.popen("df | grep '/dev/sdc1'").read().split(' ')
    #print rawCommand

    valueIndex = 0
    for i in range(1, len(rawCommand)):
        if (rawCommand[i] != ""):
            valueIndex = i
            break

    #print valueIndex, rawCommand[valueIndex]

    totalStorage = round(int(rawCommand[valueIndex]) / 1024.0 / 1024.0, 1)
    full = round(int(rawCommand[valueIndex + 2]) / 1024.0 / 1024.0, 1)

    os.popen("echo " + str(totalStorage) + " > /home/pieter/.XmobarValues/totalStorage") #save total storage
    os.popen("echo " + str(full) + " > /home/pieter/.XmobarValues/full") #save full storage

    #print totalStorage, int(totalStorage)

    time.sleep(7)
import time
from datetime import datetime
import RPi.GPIO as GPIO
import requests
import json
import serial
import os
import struct

import gps, os, time

cr=0x0D
lf=0x0A

store_name=0
store_add=1
cnt=0
out = " "

ser1 = serial.Serial("/dev/ttyUSB0", timeout=2)

print ser1

ser1.flushInput()

ser1.write("Frequency Weighting?")
ser1.write('\r\n')

while ser1.inWaiting() > 0:
      out += ser1.read(1)
      print out

ser1.write("Store Name,")
ser1.write(str(store_name))
ser1.write('\r\n')

ser1.write("Manual Address,")
ser1.write(str(store_add))
ser1.write('\r\n')

ser1.write("Measurement Time Preset Manual,1m")
ser1.write('\r\n')

ser1.write("Measure,Start")
ser1.write('\r\n')

while cnt<10:

        time.sleep(10);
        cnt=cnt+1;

ser1.write("Measure,Stop")
ser1.write('\r\n')

ser1.write("Manual Store,Start")
ser1.write('\r\n')

#ser1.write("Manual Store,Start")
#ser1.write('\r\n')

out=" "

while ser1.inWaiting() > 0:
      out += ser1.read(1)
try:
      noise= out.splitlines()[1]
except:
      print "No Response from the Machine"

print noise

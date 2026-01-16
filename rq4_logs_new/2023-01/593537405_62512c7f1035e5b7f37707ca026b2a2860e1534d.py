#!/usr/bin/python
import serial
import logging

#logging.basicConfig(filename='status.log',
#	filemode='a',
#	format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
#	datefmt='%H:%M:%S',
#	level=logging.DEBUG)

ser = serial.Serial('/dev/ttyUSB0',115200)

ser.write(b'get_status\n')
line = ser.readline()

linetext = line.decode("utf-8")
#linetext = line

#logging.info(linetext)

print(linetext,end='')

ser.close()
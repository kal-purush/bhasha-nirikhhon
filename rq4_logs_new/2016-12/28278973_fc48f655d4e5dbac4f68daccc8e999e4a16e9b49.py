#!/usr/bin/env python
import serial

# python -m serial.tools.list_ports
ser = serial.Serial('/dev/ttyACM0', 115200, timeout=5)
#ser = serial.Serial('COM7', 9600, timeout=5)

ser.write((chr(192)+chr(192)).encode('latin1'))
ser.flush()

for i in range(64*32):
    s = chr(128) + chr((i % 64) * 4) + chr(0) + chr(0)
    s = chr(0) + chr(140) + chr(0) + chr(0)
    ser.write(s.encode('latin1'))

ser.flush()
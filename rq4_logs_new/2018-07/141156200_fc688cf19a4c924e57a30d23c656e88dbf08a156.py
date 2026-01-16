__author__ = 'vedacool'

import serial

ser = serial.Serial(None, 9600)


def initialize(Comm_P, Baud_Rate):
    global ser
    # ser = serial.Serial("/dev/ttyUSB0", Baud_Rate, serial.EIGHTBITS, serial.PARITY_NONE, serial.STOPBITS_ONE, None)
    ser = serial.Serial(Comm_P, Baud_Rate, serial.EIGHTBITS, serial.PARITY_NONE, serial.STOPBITS_ONE, None)
    return ser


def receive():
    data_R = ser.readline()
    return data_R
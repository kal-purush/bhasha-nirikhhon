from machine import UART
import machine
import pycom
import os

uart = UART(0, baudrate=115200)
os.dupterm(uart)
pycom.heartbeat(False)
machine.main('main.py')
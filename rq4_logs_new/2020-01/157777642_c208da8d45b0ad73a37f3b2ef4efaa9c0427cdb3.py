#!/bin/python

import json
import time

from Phidget22.Devices import VoltageRatioInput

# copied from loadcell_to_csv.py
def connect_to_device():
    # Connect to device. Make sure anything else reading it is closed.
    ch = VoltageRatioInput.VoltageRatioInput()
    ch.close()
    ch.setDeviceSerialNumber(533042)
    ch.setChannel(1)

    ch.open() # not sure if needed
    ch.openWaitForAttachment(800)
    time.sleep(1.5)

    print("trying to read voltage ratio:")
    print(ch.getVoltageRatio())
    print("setting data interval to 8ms")
    ch.setDataInterval(8)
    return ch


ch = connect_to_device()

print("voltage now: ", ch.getVoltageRatio())

input("press enter when you have ZERO pressure on it")
zero = ch.getVoltageRatio()

psi = float(input("Put the air on the thing, and enter its psi here: \n PRESSURE(psi) : "))
bar = ch.getVoltageRatio()
print("voltage now: ", ch.getVoltageRatio())

print("saving to callibrate-pressure.json")

json.dump({'zero': zero, 'bar': bar, 'psi': psi}, open('callibrate-pressure.json', 'w'))
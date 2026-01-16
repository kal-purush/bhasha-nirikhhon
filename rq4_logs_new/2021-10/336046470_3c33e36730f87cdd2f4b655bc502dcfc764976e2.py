
#FORK BY 51M30N
#FOR ONAVIO KALEIDOS
#12/10/21




#RPi PIN OUT
#I2C Pins 
#GPIO2 -> SDA
#GPIO3 -> SCL

#Import the Library Requreid 
import smbus
import time
import argparse
import math

from pythonosc import dispatcher
from pythonosc import osc_server

# for RPI version 1, use "bus = smbus.SMBus(0)"
bus = smbus.SMBus(1)

# This is the adress we setup in the Arduino Program
#Slave Address 1
address = 0x11
port = 5011

def writeNumber(value):
    bus.write_byte(address, value)
    # bus.write_byte(address_2, value)
    # bus.write_byte_data(address, 0, value)
    return -1

def readNumber():
    # number = bus.read_byte(address)
    number = bus.read_byte_data(address, 1)
    return number

def send_set_zero(unused_addr, *args): # OK
    data = "Z {}".format(args[0])
    data_list = list(data)
    for i in data_list:
    	#Sends to the Slaves 
        writeNumber(int(ord(i)))
        time.sleep(.001)

    writeNumber(int(0x0A))
    print("SetZero = {}".format(args[0]))

def send_vitesse_value(unused_addr, *args): # OK
    data = "S {} {} {} ".format(*args)
    data_list = list(data)
    for i in data_list:
    	#Sends to the Slaves 
        writeNumber(int(ord(i)))
        #print(i)
        time.sleep(.001)

    writeNumber(int(0x0A))
    print("send_vitesse_value {} {} {} ".format(*args))


def send_mouvement_value(unused_addr, *args): # OK
    data = "M {} {} {} ".format(*args)
    data_list = list(data)
    for i in data_list:
    	#Sends to the Slaves 
        writeNumber(int(ord(i)))
        time.sleep(.001)

    writeNumber(int(0x0A))
    print("send_mouvement_value = {} {} {} ".format(*args))

def send_acceleration_value(unused_addr, *args): # OK
    data = "A {} {} {} ".format(*args)
    data_list = list(data)
    for i in data_list:
    	#Sends to the Slaves 
        writeNumber(int(ord(i)))
        #print(i)
        time.sleep(.001)

    writeNumber(int(0x0A))
    print("send_acceleration_value {} {} {} ".format(*args))


def change_direction(unused_addr, *args): # OK
    data = "D {} {} {} ".format(*args)
    data_list = list(data)
    for i in data_list:
        #Sends to the Slaves 
        writeNumber(int(ord(i)))
        #print(i)
        time.sleep(.001)

    writeNumber(int(0x0A))
    print("change_direction {} {} {} ".format(*args))


def send_run(unused_addr, *args):  # OK
    data = "R {}".format(args[0])
    data_list = list(data)
    for i in data_list:
    	#Sends to the Slaves 
        writeNumber(int(ord(i)))
        time.sleep(.001)

    writeNumber(int(0x0A))
    print("send_run = {}".format(args[0]))

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--ip",
      default="127.0.0.1", help="The ip to listen on")
  parser.add_argument("--port",
      type=int, default=port, help="The port to listen on")
  args = parser.parse_args()

  dispatcher = dispatcher.Dispatcher()
  dispatcher.map("/zero", send_set_zero) #Z
  dispatcher.map("/maxspeed", send_vitesse_value) #S
  dispatcher.map("/moveto", send_mouvement_value) #M
  dispatcher.map("/acceleration", send_acceleration_value) #A
  dispatcher.map("/run", send_run) #R
  dispatcher.map("/dir", change_direction) #D

  server = osc_server.ThreadingOSCUDPServer(
      (args.ip, args.port), dispatcher)
  print("Serving on {}".format(server.server_address))
  server.serve_forever()

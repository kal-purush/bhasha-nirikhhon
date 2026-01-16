#import os
#import socket
#import SSD1306
#from machine import ADC
#from machine import Pin, I2C
import time
import machine
import pycom
from pycoproc_2 import Pycoproc
from mqtt import MQTTClient_lib as MQTTClient
from network import WLAN
from SI7006A20 import SI7006A20
from MPL3115A2 import MPL3115A2,ALTITUDE,PRESSURE



pycom.heartbeat(False)
pycom.rgbled(0xA0009)
OLED_WIDTH = 128
OLED_HEIGHT = 64
py = Pycoproc()

if wlan.isconnected():
    pycom.rgbled(0x000F00)
else:
    pycom.rgbled(0x0F000)

"""
#I2C
i2c = I2C(0)
i2c = I2C(0, I2C.MASTER)
i2c = I2C(0, pins=('P9','P10')) # create and use default PIN assignments (P9=SDA, P10=SCL)
i2c.init(I2C.MASTER, baudrate=10000) # init as a master
"""

#Internal temp
si = SI7006A20(py)
mp = MPL3115A2(py,mode=ALTITUDE) # Returns height in meters. Mode may also be set to PRESSURE, returning a value in Pascals
mpp = MPL3115A2(py,mode=PRESSURE) # Returns pressure in Pa. Mode may also be set to ALTITUDE, returning a value in meters


#MQTT
def sub_cb(topic, msg):
   print(msg)

client = MQTTClient("sensor", "192.168.10.30", port=1883, keepalive=300)
client.set_callback(sub_cb)
client.connect()


"""
#initalize the ssd1306 oled screen
oled = SSD1306.SSD1306_I2C(OLED_WIDTH, OLED_HEIGHT, i2c)
black = 0x000000 # black color

# draw a black rectangle as a way to clear the screen
def clear_oled(oled):
    oled.fill_rect(0,0,OLED_WIDTH,OLED_HEIGHT,black)
"""

while True:
    temperature = si.temperature()
    altitude = mp.altitude()
    pressure = mpp.pressure()
    humidity = si.humidity()
    dew_point = si.dew_point()
    battery_left = py.read_battery_voltage()
    client.publish(topic="Temperature", msg=str(temperature))
    client.publish(topic="Altitude", msg=str(altitude))
    client.publish(topic="Pressure", msg=str(pressure))
    client.publish(topic="humidity", msg=str(humidity))
    client.publish(topic="Dew_point", msg=str(dew_point))
    client.publish(topic="Battery", msg=str(battery_left))
    time.sleep(300)

    #time.sleep(5)
    #clear_oled(oled)
    #oled.text(str(celcius), 0, 0)
    #oled.show()
    #clear_oled(oled)
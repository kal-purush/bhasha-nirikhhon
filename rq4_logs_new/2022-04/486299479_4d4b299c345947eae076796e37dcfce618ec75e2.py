def do_connect():
    import network
    sta_if = network.WLAN(network.STA_IF)
    if not sta_if.isconnected():
        print('connecting to network...')
        sta_if.active(True)
        sta_if.connect('My_SS_ID', '*********')
        while not sta_if.isconnected():
            pass
    print('network config:', sta_if.ifconfig())

do_connect()

from machine import Pin
import json
import socket

addr = socket.getaddrinfo('192.168.1.143', 80)[0][-1]

s = socket.socket()
s.bind(addr)
s.listen(0)

print('listening on', addr)

import time, onewire, ds18x20
import urequests

def TaskOne(c):
    try:
        s.settimeout(0.1)
        cl, addr = s.accept()
        print('client connected from', addr)
        button_press = {"text" : "ESP32", "Temperature": c}
        dataFromClient = cl.recv(1024)
        print(dataFromClient.decode())
        cl.send('HTTP/1.0 200 OK\r\nContent-type: text/html\r\n\r\n')
        cl.send(json.dumps(button_press))
        cl.close()
    except OSError:
        print('Task One timed out')

def temperature_sensor():
    ds_pin = Pin(4)
    ds_sensor = ds18x20.DS18X20(onewire.OneWire(ds_pin))
    time.sleep(0.09)
    roms = ds_sensor.scan()
    print('Found DS devices: ', roms)
    time.sleep(0.09)
    ds_sensor.convert_temp()
    for rom in roms:
        print(rom)
        print('temperature',ds_sensor.read_temp(rom))
        time.sleep(0.09)
        return ds_sensor.read_temp(rom)
while True:
    TaskOne(temperature_sensor())
    time.sleep(0.09)
    
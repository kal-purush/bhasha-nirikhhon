import network
import machine
import time
import ujson
from umqtt.simple import MQTTClient
import esp32
import ntptime

#mqtt broker
aws_broker=b"a2bftfnjlc07q-ats.iot.us-east-1.amazonaws.com"
clientid='therm1'
private_key='therm1.private.key'
private_cert='therm1.cert.pem'
pub_topic='temp/therm1'
key=None
cert=None

with open(private_key, 'r') as f:
    key = f.read()
with open(private_cert, 'r') as f:
    cert = f.read()
    
sslp = {"key":key, "cert":cert, "server_side":False}#ssl parameters

wlan=network.WLAN(network.STA_IF)
try:
    print('ESP_32 WiFi program')
    wlan.active(True)
    wlan.connect(ssid,psk)
    while not wlan.isconnected():
        machine.idle()
    print("connceted to wlan {} with ip:{}".format(ssid,wlan.ifconfig()[0]))
    ntptime.settime()
    
    print("Begin connection with MQTT Broker :: {}".format(aws_broker))
    mqtt = MQTTClient(client_id=clientid, server=aws_broker,port=8883,keepalive=1200,ssl=True,ssl_params=sslp)
    mqtt.connect()
    print("Connected to MQTT  Broker :: {}".format(aws_broker))
    
    while True:
            temp=esp32.raw_temperature()
            t="{}:{}:{}".format(time.localtime()[3],time.localtime()[4],time.localtime()[5])
            mssg=ujson.dumps({"temp":temp,"time":t})
            mqtt.publish(pub_topic,mssg)
            time.sleep(2)
    
except KeyboardInterrupt:
    print('program exit with CTRL+C')
finally:
    print('exit program')
    wlan.disconnect()
    machine.reset()
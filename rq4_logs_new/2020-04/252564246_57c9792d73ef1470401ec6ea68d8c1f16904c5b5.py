
#!/usr/bin/python
import Adafruit_DHT
import MySQLdb
import base64
import os
import time
import sys
from i2csensorcode import atlas_i2c
from string import letters
import pigpio
import DHT22

phdevice = atlas_i2c()

os.system('modprobe w1-gpio')
os.system('modprobe w1-therm')


password = base64.b64decode("cm9uam9uam8=")

db = MySQLdb.connect("localhost","sensorData",password,"sensor_data")


device=["28-021502d4ddff","28-02150397b5ff"]
length=len(device)
temperatureH2O = [0,1]


def readtemp():
    c = 0
    while c<length:
        tempfile=('/sys/bus/w1/devices/' + device[c] + '/w1_slave')
        temp=open(tempfile)
        lines=temp.readlines()
        temp.close()
        if lines[0].find("YES"):
            delimeter=lines[1].find('=')
            temperatureC=(float(lines[1][delimeter+1:delimeter+6])/1000)
	    temperatureC=(float(lines[1][delimeter+1:delimeter+6])/1000)
            tempC=round(temperatureC, 2)
            temperatureF=round(temperatureC*1.8+32, 2)
	    temperatureH2O[c]=temperatureF
            c += 1
        else:
            temperatureH2O[c]=error
            c += 1
	if c == 1:
	    phtemp = "T,{}".format(tempC)
            (phdevice.query(phtemp))

readtemp()

input = "R"
ph = (phdevice.query(input))
ph2 = ph.translate(None, letters)
ph3 = float(ph2)

#sensor = Adafruit_DHT.AM2302
#pin = 5
#pin2 = 6

#humidity, ctemperature = Adafruit_DHT.read_retry(sensor, pin)
#humidity2, ctemperature2 = Adafruit_DHT.read_retry(sensor, pin2)

humidity = [0,1]
ctemperature = [0,1]

pi = pigpio.pi()
r = 0
pin = [5,6]
while r < 2:
   s = DHT22.sensor(pi, pin[r])
   s.trigger()
   time.sleep(0.2)
   humidity[r] = (s.humidity())
   ctemperature[r] = (s.temperature())
   r += 1
   s.cancel()
pi.stop()

ltemperature=ctemperature[0]*1.8+32
temperature = round(ltemperature, 2)
humidity=round(humidity[0], 2)

ltemperature2=ctemperature[1]*1.8+32
temperature2 = round(ltemperature2, 2)
humidity2=round(humidity[1], 2)

#ltemperature=ctemperature*1.8+32
#temperature = round(ltemperature, 2)

#humidity = round(humidity, 2)

#ltemperature2=ctemperature2*1.8+32
#temperature2 = round(ltemperature2, 2)

#humidity2 = round(humidity2, 2)

reading=[0,1,2,3,4,5,6]


if humidity is not None:
    reading[1]=humidity
else:
    reading[1] = 'failed'

if temperature is not None:
    reading[0]=temperature
else:
    reading[0] = 'failed'

if temperature2 is not None:
    reading[2]=temperature2
else:
    reading[2] = 'failed'

if humidity2 is not None:
    reading[3]=humidity2
else:
    reading[3] = 'failed'

reading[4] = temperatureH2O[0]
reading[5] = temperatureH2O[1]
reading[6] = round(ph3, 2)
cursor=db.cursor()

write = "INSERT INTO TempData(Temp, Humidity, Temp2, Humidity2, h2oTemp1, h2oTemp2, ph) VALUES (%s,%s,%s,%s,%s,%s,%s)"
args = reading
cursor.execute(write, args)
db.commit()
db.close()

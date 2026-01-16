import Adafruit_DHT
import RPi.GPIO as GPIO
import time
import RawObject
import json
import datetime
import os

#Saves data as filename at path, checking if path exists and
#creating it if doesn't
def saveData (filename,path,data):
        if os.path.isdir (path):
                file = open (path +"/" + filename,"w")
                file.write (data)
                file.close
        else:
                os.mkdir (path)
                saveData (filename,
                          path,data)


## Setting up things
GPIO.setmode(GPIO.BOARD)
rainSensorPine = 11
tempSensor = Adafruit_DHT.DHT11
tempSensorPine = 22
refresh = 60
GPIO.setup(11, GPIO.IN)

pathSaveData = os.path.expanduser ("~") +"/SensorData"

print ("Aeolus Data Monitor\n")
print ("Reading values from Sensors")

while (1):
	hum, temp = Adafruit_DHT.read_retry(tempSensor, tempSensorPine)
	## When GPIO 11 goes to 0, it detected a rain drop
	rain = not GPIO.input(rainSensorPine)

	if hum is not None and temp is not None:
		## Show status on console
		print ("Temp = {0:0.1f} and Hum = {1:0.1f}\n").format(temp, hum)
		if rain:
			print ("Raining")

	        ## Create RawData
    		rawDHT11 = {
        		"temp" : temp,
         		"hum" : hum
    		}

	        rawYL83 = {
        		"rain" : rain
	        }

    		## RawObject gets created
	    	dht11Object = RawObject.RawObject (rawDHT11,"DHT11",unicode(
        	    	datetime.datetime.now()
            		.strftime("%Y-%m-%d %H:%M:%S")))

	        yl83Object = RawObject.RawObject (rawYL83, "YL83", unicode(
        	        datetime.datetime.now()
                	.strftime("%Y-%m-%d %H:%M:%S")))

	    	## Data gets saved at ~/SensorData
    		saveData (dht11Object.getDate() + ".json",
            		pathSaveData, dht11Object.getJsonData() )

	        saveData (yl83Object.getDate() + "C.json",
          		pathSaveData, yl83Object.getJsonData() )

   		## Sender will be launched at another thread.

	else:
		print ("Temp/Hum Retrieve Failed")

	time.sleep(refresh)
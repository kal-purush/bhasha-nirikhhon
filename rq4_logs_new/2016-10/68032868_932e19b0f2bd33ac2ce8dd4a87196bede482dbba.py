import Adafruit_DHT
import RPi.GPIO as GPIO
import time

GPIO.setmode(GPIO.BOARD)
rainSensorPine = 11
tempSensor = Adafruit_DHT.DHT11
tempSensorPine = 22
refresh =

print ("Aeolus Data Monitor\n")
print ("Reading values from Sensors")

while (1):
	hum, temp = Adafruit_DHT.read_retry(tempSensor, tempSensorPine)
    ## When GPIO 11 goes to 0, it detected a rain drop
    rain = not GPIO.input(rainSensorPine)

	if hum is not None and temp is not None:
		print ("Temp = {0:0.1f} and Hum = {1:0.1f}\n").format(temp, hum);

        ## Create RawObject

        ## Send json

	else:
		print ("Temp/Hum Retrieve Failed")

	time.sleep(60)
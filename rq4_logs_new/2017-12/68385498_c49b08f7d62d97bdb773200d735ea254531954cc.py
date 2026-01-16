import time
import RPi.GPIO as GPIO

GPIO.setmode(GPIO.BCM)
GPIO.setup(17,GPIO.OUT)
GPIO.setup(22,GPIO.OUT)
GPIO.setup(27,GPIO.OUT)
GPIO.output(17,GPIO.HIGH)
GPIO.output(22,GPIO.HIGH)
GPIO.output(27,GPIO.HIGH)
time.sleep(2)

GPIO.cleanup()


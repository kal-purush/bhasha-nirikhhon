import RPi.GPIO as GPIO
from time import sleep
 

GPIO.setmode(GPIO.BCM)
GPIO.setup(17, GPIO.OUT)

while True:
    # output to pin 17
    GPIO.output(17, GPIO.HIGH)
    # input_value = GPIO.input(27)
    # print(input_value)
    sleep(0.2)
    GPIO.output(17, GPIO.LOW)
    sleep(0.2)
    # input_value = GPIO.input(27)
    # print(input_value)
    # sleep(1)

# msg = "Done"
# print(msg)

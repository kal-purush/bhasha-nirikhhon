import RPi.GPIO as GPIO
import time


GPIO.setmode(GPIO.BCM)

input_1 = 23
input_2 = 24
clock = 25

GPIO.setup(input_1, GPIO.OUT)
GPIO.setup(input_2, GPIO.OUT)
GPIO.setup(clock, GPIO.OUT)

pwm = GPIO.PWM(clock, 1000)

pwm.start(25)

def forward(speed):
    pwm.ChangeDutyCycle(speed)
    GPIO.output(input_1,GPIO.HIGH)
    GPIO.output(input_2,GPIO.LOW)

def reverse(speed):
    pwm.ChangeDutyCycle(speed)
    GPIO.output(input_1,GPIO.LOW)
    GPIO.output(input_2,GPIO.HIGH)

try:
    while True:
        forward(50)
        time.sleep(2)
        reverse(50)
        time.sleep

except KeyboardInterrupt:
    pwm.stop()
    GPIO.cleanup()
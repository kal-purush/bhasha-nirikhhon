import RPi.GPIO as GPIO
from time import sleep

GPIO.setmode(GPIO.BCM)

#Buttons Setup
#GPIO.setup(buttonPin, GPIO.IN, pull_up_down=GPIO.PUD_UP)


#Motor Scanner Setup
stepPin1 = 2
dirPin1 = 3
#enablePin1 = 18
sleepPin1 = 4

GPIO.setup(stepPin1, GPIO.OUT)
GPIO.setup(dirPin1, GPIO.OUT)
#GPIO.setup(enablePin1, GPIO.OUT)
GPIO.setup(sleepPin1, GPIO.OUT)

#GPIO.output(enablePin1, GPIO.LOW)
GPIO.output(sleepPin1, GPIO.LOW)
GPIO.output(dirPin1, GPIO.HIGH)


#Motor Vertical
stepPin2 = 27
dirPin2 = 22
#enablePin2 = 18
sleepPin2 = 17

GPIO.setup(stepPin2, GPIO.OUT)
GPIO.setup(dirPin2, GPIO.OUT)
#GPIO.setup(enablePin2, GPIO.OUT)
GPIO.setup(sleepPin2, GPIO.OUT)

#GPIO.output(enablePin2, GPIO.LOW)
GPIO.output(sleepPin2, GPIO.LOW)
GPIO.output(dirPin2, GPIO.HIGH)


#Motor Horizontal
stepPin3 = 9
dirPin3 = 11
#enablePin2 = 18
sleepPin3 = 10

GPIO.setup(stepPin3, GPIO.OUT)
GPIO.setup(dirPin3, GPIO.OUT)
#GPIO.setup(enablePin3, GPIO.OUT)
GPIO.setup(sleepPin3, GPIO.OUT)

#GPIO.output(enablePin3, GPIO.LOW)
GPIO.output(sleepPin3, GPIO.LOW)
GPIO.output(dirPin3, GPIO.HIGH)

delay = .0005


#Moving Scanner Motor
def moveScannerUp(num):
    #step_count = input("Enter number of steps: ")
    step_count = num
    GPIO.output(dirPin1, GPIO.LOW)
    GPIO.output(sleepPin1, GPIO.HIGH)
    for x in range(step_count):
        GPIO.output(stepPin1, GPIO.HIGH)
        sleep(delay) 
        GPIO.output(stepPin1, GPIO.LOW)
        sleep(delay)
    GPIO.output(sleepPin1, GPIO.LOW)

def moveScannerDown(num):
    #step_count = input("Enter number of steps: ")
    step_count = num
    GPIO.output(dirPin1, GPIO.HIGH)
    GPIO.output(sleepPin1, GPIO.HIGH)
    for x in range(step_count):
        GPIO.output(stepPin1, GPIO.HIGH)
        sleep(delay) 
        GPIO.output(stepPin1, GPIO.LOW)
        sleep(delay)
    GPIO.output(sleepPin1, GPIO.LOW)


#Moving Vertical Motor
def moveVerticalUp(num):
    print"yola"
    #step_count = input("Enter number of steps: ")
    step_count = num
    GPIO.output(dirPin2, GPIO.LOW)
    GPIO.output(sleepPin2, GPIO.HIGH)
    for x in range(step_count):
        GPIO.output(stepPin2, GPIO.HIGH)
        sleep(delay) 
        GPIO.output(stepPin2, GPIO.LOW)
        sleep(delay)
    GPIO.output(sleepPin2, GPIO.LOW)

def moveVerticalDown(num):
    #step_count = input("Enter number of steps: ")
    step_count = num
    GPIO.output(dirPin2, GPIO.HIGH)
    GPIO.output(sleepPin2, GPIO.HIGH)
    for x in range(step_count):
        GPIO.output(stepPin2, GPIO.HIGH)
        sleep(delay) 
        GPIO.output(stepPin2, GPIO.LOW)
        sleep(delay)
    GPIO.output(sleepPin2, GPIO.LOW)

#Moving Horizontal Motor
def moveHorizontalUp(num):
    #step_count = input("Enter number of steps: ")
    step_count = num
    GPIO.output(dirPin3, GPIO.LOW)
    GPIO.output(sleepPin3, GPIO.HIGH)
    for x in range(step_count):
        GPIO.output(stepPin3, GPIO.HIGH)
        sleep(delay) 
        GPIO.output(stepPin3, GPIO.LOW)
        sleep(delay)
    GPIO.output(sleepPin3, GPIO.LOW)

def moveHorizontalDown(num):
    #step_count = input("Enter number of steps: ")
    step_count = num
    GPIO.output(dirPin3, GPIO.HIGH)
    GPIO.output(sleepPin3, GPIO.HIGH)
    for x in range(step_count):
        GPIO.output(stepPin3, GPIO.HIGH)
        sleep(delay) 
        GPIO.output(stepPin3, GPIO.LOW)
        sleep(delay)
    GPIO.output(sleepPin3, GPIO.LOW)



while(1):

    command = raw_input("Enter command: ")
    steps = input("Enter number of steps: ")

    if command == "move scanner up":
       moveScannerUp(steps)
       print "moveSca"

    if command == "move scanner down":
       moveScannerDown(steps)
       print "moveSca"
       
    if command == "move vertical up":
       moveVerticalUp(steps)
       print "moveSca"
       
    if command == "move vertical down":
       moveVerticalDown(steps)
       print "moveSca"
       
    if command == "move horizontal up":
       moveHorizontalUp(steps)
       print "moveSca"
       
    if command == "move horizontal down":
       moveHorizontalDown(steps)
       print "moveSca"    
       


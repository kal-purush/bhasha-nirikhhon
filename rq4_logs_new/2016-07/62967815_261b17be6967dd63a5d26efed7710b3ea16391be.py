import RPi.GPIO as GPIO
from time import sleep

class DCMotor(object):
    A = -1
    B = -1
    E = -1
    __pwm = None

    def __init__(self, mode, a, b, en):
        self.A = a
        self.B = b
        self.E = en
        GPIO.setmode(mode)
        GPIO.setup(a, GPIO.OUT)
        GPIO.setup(b, GPIO.OUT)
        GPIO.setup(en, GPIO.OUT)
        self.__pwm = GPIO.PWM(en, 100)
        self.__pwm.start(0)

    def swap():
        temp = self.A
        self.A = self.B
        self.B = temp

    def forward(self, dc):
        GPIO.output(self.A, GPIO.HIGH)
        GPIO.output(self.B, GPIO.LOW)
        if (0 <= dc <= 100):
            self.__pwm.ChangeDutyCycle(dc)

    def backward(self, dc):
        GPIO.output(self.A, GPIO.LOW)
        GPIO.output(self.B, GPIO.HIGH)
        if (0 <= dc <= 100):
            self.__pwm.ChangeDutyCycle(dc)

    def stop(self):
        self.__pwm.stop()

    def changeSpeed(self, dc):
        if (0 <= dc <= 100):
            self.__pwm.ChangeDutyCycle(dc)

class Servo(object):
    P = -1
    __pwm = None
    __zero = 3
    __slope = 0.04722
    __restrictions = [0, 360]

    def __init__(self, mode, p, freq = 50, zero = 3, slope = 0.04722, lower = 0, upper = 360):
        self.P = p
        self.__restrictions = [lower, upper]
        self.__zero = zero
        self.__slope = slope
        GPIO.setmode(mode)
        GPIO.setup(p, GPIO.OUT)
        self.__pwm = GPIO.PWM(p, freq)
        self.__pwm.start(7.25)

    def config(self, d0, d180):
        if (d180 > d0 > 0):
            self.__slope = (d180 - d0) / 180
            self.__zero = d0
        else:
            raise ValueError("d180 > d0 > 0 should be True")

    def changeFreq(self, freq):
        if (freq > 0):
            self.__pwm.ChangeFrequency(freq)

    def changeDutyCycle(self, dc):
        if (0 <= dc <= 100):
            self.__pwm.ChangeDutyCycle(dc)

    def changeRestrictions(self, lower, upper):
        if (0 <= lower <= upper <= 360):
            self.__restrictions = [lower, upper]
        else:
            raise ValueError("lower and upper values should be between 0 and 360, and upper >= lower")

    def turn(self, angle):
        if (self.__restrictions[0] <= angle <= self.__restictions[1]):
            self.__pwm.ChangeDutyCycle((float(angle) * self.__slope) + self.__zero)
        elif (0 <= angle < self.__restrictions[0]): 
            self.turn(self.__restrictions[0])
        elif (self.__restrictions[1] < angle <= 360): 
            self.turn(self.__restrictions[1])
        else:
            pass

class SimpleMotor(object):
    A = -1
    B = -1

    def __init__(self, mode, a, b):
        self.A = a
        self.B = b
        GPIO.setmode(mode)
        GPIO.setup(a, GPIO.OUT)
        GPIO.setup(b, GPIO.OUT)

    def forward(self):
        GPIO.output(self.A, GPIO.HIGH)
        GPIO.output(self.B, GPIO.LOW)

    def backward(self):
        GPIO.output(self.A, GPIO.LOW)
        GPIO.output(self.B, GPIO.HIGH)

    def stop(self):
        GPIO.output(self.A, GPIO.LOW)
        GPIO.output(self.B, GPIO.LOW)

    def swap(self):
        temp = self.A
        self.A = self.B
        self.B = temp

class LED(object):
    PIN = -1

    def __init__(self, mode, pin):
        self.PIN = pin
        GPIO.setmode(mode)
        GPIO.setup(pin, GPIO.OUT)

    def on(self):
        GPIO.output(self.PIN, GPIO.HIGH)

    def off(self):
        GPIO.output(self.PIN, GPIO.LOW)

    def flash(self, duration = 0.01):
        self.on()
        sleep(duration)
        self.off()

    def blink(self, blinks, duration = 0.1, interval = 0.1):
        for i in range(1, blinks + 1):
            self.flash(duration = duration)
            sleep(interval)
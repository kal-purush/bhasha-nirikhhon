'''
A Button agent lib for the raspberry.
Part of PiBuzz
https://github.com/actuino/PiBuzz
'''

import atexit
import time
import math
from sys import exit, version_info

try:
    import RPi.GPIO as GPIO
except ImportError:
    exit("This library requires the RPi.GPIO module\nInstall with: sudo pip install RPi.GPIO")

__version__ = '0.1.0'


BuzzerPin = 40 # Buzzer Pin
ButtonPin = 33	# Button Pin

State = '0' # Default state of the automata

# Events handlers
_on_click = None
_on_longclick = None

def setup():
	'''Initial setup of the hardwarte
	'''
	GPIO.setmode(GPIO.BOARD) # Numbers GPIOs by physical location
	GPIO.setup(BuzzerPin, GPIO.OUT)
	GPIO.output(BuzzerPin, GPIO.LOW)
	GPIO.setup(ButtonPin, GPIO.IN,pull_up_down=GPIO.PUD_UP)
	GPIO.add_event_detect(ButtonPin, GPIO.BOTH, bouncetime=50)  # add edges detection

def buzzer_on():
	GPIO.output(BuzzerPin, GPIO.HIGH)

def buzzer_off():
	GPIO.output(BuzzerPin, GPIO.LOW)

def beep(x):
	''' beeps for x seconds
	'''
	buzzer_on()
	time.sleep(x)
	buzzer_off()

def loop():
	'''Main blocking loop, 
	uses some kind of automata
	'''
	global State, Start
	while True:
		if GPIO.event_detected(ButtonPin):
			#print('Button chg', GPIO.input(ButtonPin))
			if State == '0':
				Start = time.time()
				State = 'c'
			elif State == 'c':
				#print('c',length)
				if callable(_on_click):
					_on_click(length)
				State = '0'
				length = 0
			elif State == 'L':
					#print('L',length)
					if callable(_on_click):
						_on_long_click(math.trunc(length))
					State = '0'
					length = 0
		if State != '0':
			length=time.time()-Start
		if State == 'c' and length>1:
			State='L'
		if State == 'L' and round(length*10)%10==0:
			beep(0.03) 
		time.sleep(0.1)

def click():
    '''Bind an action to short click
    The handler will receive duration of the click
    (less than 1 sec)
    '''
    def register(handler):
        global _on_click
        _on_click = handler

    return register

def long_click():
    '''Bind an action to long clicks
    The handler will receive duration of the click in seconds
    (int, 1 or more)
    '''
    def register(handler):
        global _on_long_click
        _on_long_click = handler

    return register


def _exit():
	'''Shut off buzzer and release GPIO
	'''
	GPIO.output(BuzzerPin, GPIO.LOW)
	GPIO.cleanup() # Release resource


atexit.register(_exit)

setup()
'''
#
# The Pymiento Project
#
# Project: soundWaves
# Description: uses the GPIO to read a capacitive sensor. When the CapSense is touched, it plays a beep sound.
# Autor: Iván Gonzalo Moyano Pérez
# Version: 1.0
# TODO: duplicate this for the seven CapSense and its related sounds
#
'''

import wiringpi as io
import time
import pygame as s
import atexit

s.init()
s.mixer.music.load("beep.mp3")

pin = 17
io.wiringPiSetupGpio()

io.pinMode(pin, io.OUTPUT)

@atexit.register
def goodbye():
    print "You are now leaving readCapSensAndPlaySounds.py"
    s.mixer.quit()

print "Start reading..."

while 1:
	repeats = 2
	total = 0.0

	for i in range(0, repeats):
		io.pinMode(pin, io.OUTPUT)
		io.digitalWrite(pin, io.LOW)
		
		io.pinMode(pin, io.INPUT)
		io.pullUpDnControl(pin, io.PUD_OFF)
	
		maxCycles = 1000
		cycles = 0
		
		while (cycles<maxCycles and io.digitalRead(pin)==0):
			cycles += 1
		total += cycles
	mean = total / repeats

	print mean

	if(mean < 10): 
		print ">>>>>>>>>>>>>  Touched! " + str(mean)
		if(not s.mixer.music.get_busy()):
			s.mixer.music.play()
		
	time.sleep(0.05)

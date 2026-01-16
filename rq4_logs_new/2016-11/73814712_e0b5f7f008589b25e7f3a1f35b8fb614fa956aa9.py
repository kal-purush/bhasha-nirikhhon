#!/usr/bin/env python
import RPi.GPIO as GPIO
import pygame
import time
import mpr121
import os
import LCD1602

KidRockPin = 5
Gpin   = 26
Rpin   = 23
BackGroundMusicArrayCount = 0
touches = [0,0,0,0,0,0,0,0];

snare = None
kick = None
closedhh = None
openhh = None
tom1 = None
tom2 = None
BackGroundMusic = None
BackGroundMusicArray = []
BackGroundMusicArrayCount = 0
KidRockVar = 0
MusicPaused = 0

def setup():
	# PIN Setup
	GPIO.setmode(GPIO.BCM)       # Numbers GPIOs by physical location
	GPIO.setup(Gpin, GPIO.OUT)     # Set Green Led Pin mode to output
	GPIO.setup(Rpin, GPIO.OUT)     # Set Red Led Pin mode to output
	GPIO.setup(6, GPIO.IN)		# Set interrupt Pin to input
	GPIO.setup(KidRockPin, GPIO.IN, pull_up_down=GPIO.PUD_UP)    # Set BtnPin's mode is input, and pull up to high level(3.3V)
	GPIO.add_event_detect(KidRockPin, GPIO.FALLING, callback=detect, bouncetime=300)
	#Pygame setup
	pygame.mixer.pre_init(44100, -16, 2, 512)
	pygame.mixer.init()
	pygame.mixer.music.set_volume(0.99)  
	# Initialize MPR121 here
	mpr121.TOU_THRESH = 10
	mpr121.REL_THRESH = 20
	mpr121.setup(0x5a)
	# Initialize LCD Display
	LCD1602.init(0x27, 1)
	LCD1602.clear()
	KidRock()
    
def KidRock():
	global KidRockVar
	global snare
	global kick
	global closedhh
	global openhh
	global tom1
	global tom2
	global BackGroundMusic
	global BackGroundMusicArrayCount
	global BackGroundMusicArray

	if (KidRockVar == 0):
		#LED change
		GPIO.output(Rpin, 0)
		GPIO.output(Gpin, 1)
		#load background music in an array to skip through
		BackGroundMusicArray = []
		for filename in os.listdir("samples_music/kid/"):
			BackGroundMusicArray.append("samples_music/kid/"+ filename) # 123 im Sauseschritt laden!
		BackGroundMusicArrayCount = 0
		pygame.mixer.music.load(BackGroundMusicArray[BackGroundMusicArrayCount])
		pygame.mixer.music.play(0)
		print(BackGroundMusicArrayCount)
		print(len(BackGroundMusicArray))
		#drum sounds are loaded
		kick = pygame.mixer.Sound("samples_music/kid_drums/bongo1.ogg")
		snare = pygame.mixer.Sound("samples_music/kid_drums/bongo2.ogg")
		closedhh = pygame.mixer.Sound("samples_music/kid_drums/kick.ogg")
		openhh = pygame.mixer.Sound("samples_music/kid_drums/clave.ogg")
		tom1 = pygame.mixer.Sound("samples_music/kid_drums/tamborine.ogg")
		tom2 = pygame.mixer.Sound("samples_music/kid_drums/triangle.ogg")
		#update display
		LCD1602.clear()
		LCD1602.write(0, 0, 'Kid-Mode!')
		showtitle = BackGroundMusicArray[0]
		indexofslash = showtitle.rfind("/")+1
		showtitle = showtitle[indexofslash:]
		LCD1602.write(1, 1,showtitle)
		KidRockVar = 1
	else:
		#LED change
		GPIO.output(Rpin, 1)
		GPIO.output(Gpin, 0)
		#load background music in an array to skip through
		BackGroundMusicArray = []
		for filename in os.listdir("samples_music/drumless_songs/"):
			BackGroundMusicArray.append("samples_music/drumless_songs/"+ filename)
		BackGroundMusicArrayCount = 0
		pygame.mixer.music.load(BackGroundMusicArray[BackGroundMusicArrayCount])
		pygame.mixer.music.play(0)
		print(BackGroundMusicArrayCount)
		print(len(BackGroundMusicArray))
		#drum sounds are loaded
		kick = pygame.mixer.Sound("samples_music/drums/kick-oldschool.ogg")
		snare = pygame.mixer.Sound("samples_music/drums/snare-acoustic02.ogg")
		closedhh = pygame.mixer.Sound("samples_music/drums/hihat-acoustic02.ogg")
		openhh = pygame.mixer.Sound("samples_music/drums/openhat-acoustic01.ogg")
		tom1 = pygame.mixer.Sound("samples_music/drums/tom-acoustic01.ogg")
		tom2 = pygame.mixer.Sound("samples_music/drums/tom-acoustic02.ogg")
		LCD1602.clear()
		LCD1602.write(0, 0, 'Rock-Mode!')
		showtitle = BackGroundMusicArray[0]
		indexofslash = showtitle.rfind("/")+1
		showtitle = showtitle[indexofslash:]
		LCD1602.write(1, 1,showtitle)
		KidRockVar = 0
     
def detect(chn):
	pygame.mixer.music.stop()
	KidRock()

def nextSong():
	global BackGroundMusic
	global BackGroundMusicArrayCount
	global BackGroundMusicArray
	pygame.mixer.music.stop()
	if (BackGroundMusicArrayCount != len(BackGroundMusicArray)):
		BackGroundMusicArrayCount = BackGroundMusicArrayCount+1
		pygame.mixer.music.load(BackGroundMusicArray[BackGroundMusicArrayCount])
		pygame.mixer.music.play(0)
	else:
		BackGroundMusicArrayCount = 0
		pygame.mixer.music.load(BackGroundMusicArray[BackGroundMusicArrayCount])
		pygame.mixer.music.play(0)
	LCD1602.write(1, 1, "                ")
	showtitle = BackGroundMusicArray[BackGroundMusicArrayCount]
	indexofslash = showtitle.rfind("/")+1
	showtitle = showtitle[indexofslash:]
	LCD1602.write(1, 1,showtitle)

def previousSong():
	global BackGroundMusic
	global BackGroundMusicArrayCount
	global BackGroundMusicArray
	pygame.mixer.music.stop()
	if (BackGroundMusicArrayCount != 0):
		BackGroundMusicArrayCount = BackGroundMusicArrayCount-1
		pygame.mixer.music.load(BackGroundMusicArray[BackGroundMusicArrayCount])
		pygame.mixer.music.play(0)
	else:
		BackGroundMusicArrayCount = len(BackGroundMusicArray)-1
		pygame.mixer.music.load(BackGroundMusicArray[BackGroundMusicArrayCount])
		pygame.mixer.music.play(0)
	LCD1602.write(1, 1, "               ")
	showtitle = BackGroundMusicArray[BackGroundMusicArrayCount]
	indexofslash = showtitle.rfind("/")+1
	showtitle = showtitle[indexofslash:]
	LCD1602.write(1, 1,showtitle)



def run():
	global BackGroundMusic
	global MusicPaused
	while True:
		if (GPIO.input(6)): # Interupt pin is high
			pass
		else: # Interupt pin is low
			touchData = mpr121.readData(0x5a)
			for i in range(8):
				if (touchData & (1<<i)):
					if (touches[i] == 0):
							#print( 'Pin ' + str(i) + ' was just touched')
						if (i == 0):
							kick.play()
						elif i == 1:
							snare.play()
						elif i == 2:
							openhh.play()
						elif i == 3:
							closedhh.play()
						elif i == 4:
							tom1.play()
						elif i == 5:
							tom2.play()
						elif i == 6:
							previousSong()
						elif i == 7:
							if (MusicPaused == 0):
								pygame.mixer.music.pause()
								MusicPaused = 1
							else:
								pygame.mixer.music.unpause()
								MusicPaused = 0
						elif i == 8:
							nextSong()
					touches[i] = 1
				else:
					if (touches[i] == 1):
						print( 'Pin ' + str(i) + ' was just released')
					touches[i] = 0;

def destroy():
	GPIO.output(Gpin, GPIO.HIGH)       # Green led off
	GPIO.output(Rpin, GPIO.HIGH)       # Red led off
	GPIO.cleanup()                     # Release resource
	LCD1602.clear()
	LCD1602.write(0,0,"Good bye,")
	LCD1602.write(1,1,"Niklas")

if __name__ == '__main__':     # Program start from here
	setup()
	try:
		run()
	except KeyboardInterrupt:  # When 'Ctrl+C' is pressed, the child program destroy() will be  executed.
		destroy()
# Write your code here :-)
import RPi.GPIO as GPIO
import os
from time import sleep     # this lets us have a time delay
import random
import pygame

#pygame.init()
GPIO.setmode(GPIO.BOARD)
GPIO.setup(7, GPIO.IN)
GPIO.setup(16, GPIO.IN)
GPIO.setup(15, GPIO.IN)

vineHappyNames = ['freshavocado_vine.wav', 'wednesday_vine.wav', 'look_at_all_those_chickens_vine.wav', 'merry_chrysler_vine.wav', 'why_you_always_lyin_vine.wav']

vineNeutralNames = ['adam_vine.wav', 'hi_my_name_is_trey_vine.wav', 'hi_welcome_to_chilis_vine.wav',  'its_an_avocado_vine.wav', 'more_like_hurricane_tortilla_vine.wav']

vineSadNames = ['life_lemons_vine.wav', 'road_work_vine.wav', 'i_couldve_dropped_my_croissant_vine.wav', 'country_boi_i_love_you_vine.wav', 'i_smell_like_beef_vine.wav']

vineHappyNamesList = []
vineNeutralNamesList = []
vineSadNamesList = []

for i in range(0, 4):
    vineHappyNamesList.append('sudo aplay /home/pi/vine-audio-project/recordings/' + vineHappyNames[i])

for i in range(0, 4):
    vineNeutralNamesList.append('sudo aplay /home/pi/vine-audio-project/recordings/' + vineNeutralNames[i])

for i in range(0, 4):
    vineSadNamesList.append('sudo aplay /home/pi/vine-audio-project/recordings/' + vineSadNames[i])


while True:
    if GPIO.input(7): 
        os.system(vineHappyNamesList[random.randint(0, 4)])
        sleep(0.1)         # wait 0.1 seconds
    if GPIO.input(16):
        os.system(vineNeutralNamesList[random.randint(0, 4)])
        sleep(0.1)         # wait 0.1 seconds
    if GPIO.input(15):
        os.system(vineSadNamesList[random.randint(0, 4)])
        sleep(0.1)         # wait 0.1 seconds
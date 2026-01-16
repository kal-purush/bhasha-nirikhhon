#!/usr/bin/env python


#pip3 install mfrc522
#pip3 install spidev


import RPi.GPIO as GPIO
from mfrc522 import SimpleMFRC522

reader = SimpleMFRC522()

try:
  id, text = reader.read()
  print(id)
  print(text)
finally:
  GPIO.cleanup()
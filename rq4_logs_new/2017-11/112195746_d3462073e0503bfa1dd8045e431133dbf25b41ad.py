import RPi.GPIO as GPIO
import time

LedPin = 11    # pin11

def setup():
  GPIO.setmode(GPIO.BOARD)       # Numbers GPIOs by physical location
  GPIO.setup(LedPin, GPIO.OUT)   # Set LedPin's mode is output
  GPIO.output(LedPin, GPIO.HIGH) # Set LedPin high(+3.3V) to turn on led

if __name__ == '__main__':     # Program start from here
  setup()
  GPIO.output(LedPin, GPIO.HIGH)  # led on

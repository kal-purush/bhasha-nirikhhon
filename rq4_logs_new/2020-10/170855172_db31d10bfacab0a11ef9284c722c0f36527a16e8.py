import RPi.GPIO as GPIO
import time
import numpy as np

# define the GPIO pins for trigger and echo
TRIG = 17 # pin 11
ECHO = 22 # pin 15


def get_distance(prev_values):
  GPIO.output(TRIG, True)
  time.sleep(.00001)
  GPIO.output(TRIG, False)
  while GPIO.input(ECHO) == False:
    start = time.time()
  while GPIO.input(ECHO) == True:
    end = time.time()
  try:
    sig_time = end - start
    new_distance = sig_time /0.000058
    prev_values = prev_values[1:] + [new_distance]
    distance = np.mean(prev_values) # taking moving average
    print(" Distance is {} cm".format(distance))
    return prev_values, distance
  except:
    new_distance = 10 # considereing new distance is 20cm or more
    prev_values = prev_values[1:] + [new_distance]
    distance = np.mean(prev_values) # taking moving average
    print("object far away")
    return prev_values, distance


if __name__ == "__main__":

  GPIO.setmode(GPIO.BCM)
  GPIO.cleanup()

  # define the pins as input/output
  GPIO.setup(TRIG, GPIO.OUT)
  GPIO.setup(ECHO, GPIO.IN)

  prev_values = [0,0,0,0,0,0,0] # the length of this list decides your window of moving average
  while True:
    prev_values, distance = get_distance(prev_values)
    time.sleep(.1)

  GPIO.cleanup()
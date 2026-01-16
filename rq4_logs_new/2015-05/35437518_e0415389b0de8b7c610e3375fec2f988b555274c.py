from gopigo import *
import time
import math
set_speed(100)

enc_tgt(1, 1, 18)
time.sleep(.1)
fwd()
time.sleep(3)

def move_forward(feet):
  mm = feet * 304.8
  steps = mm/11.34
  enc_tgt(1, 1, steps)
  time.sleep(.1)
  fwd()
  time.sleep(3 * feet)

def turnright():
  enc_tgt(1, 0, 14 steps)
  time.sleep(.1)
  right()
  time.sleep(.1)
  
def turnleft():
  enc_tgt(0, 1, 14 steps)
  time.sleep(.1)
  left()
  time.sleep(.1)

move_forward(3)
turnright()
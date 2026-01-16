from random import randrange
import time

DBG = True

#sorry i like colors
default   = "\x1B[0m"
bold      = "\x1B[1m"
yellow    = "\x1B[33m"
red       = "\x1B[31m"
cyan      = "\x1B[36m"
blackhole = "\x1B[48;5;232m"
orange    = "\x1B[38;5;130m"
#blackhole = "\x1B[38;5;232m"

def color(j):
  a = 38
  b = 5
  c = j #randrange(256)
  if DBG:
    print("[dbg] a=%i b=%i c=%i" % (a,b,c))
  return "\x1B[%i;%i;%im" % (a,b,c)

msg = "who dis?"
#color = "\x48[mi38 or something."
delay = 0.1

j = 0

while True:
  a = color(j) + msg
  j += 1
  print(a)
  time.sleep(delay)
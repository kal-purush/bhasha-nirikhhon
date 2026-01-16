#!/usr/bin/python3
def f(x):
  import math
  x = float(x)
  v = math.sqrt(2*x) - 0.5
  r = round(v)
  if x == (r*(r+1)/2): return r
  else: return v

from sys import argv
print(f(argv[1]))
from scipy import signal
import numpy as np
from numpy import array
import matplotlib.pyplot as plt

def dotProduct(X, Y):
    return sum(i*j for i,j in zip(X,Y))

print "** Convolution of sequences **"
print "Enter the first sequence : "
a = map(int, raw_input().split())
print "Enter the second sequence : "
b = map(int, raw_input().split())

a, b = array(a, copy=False, ndmin=1), array(b, copy=False, ndmin=1)
if (len(a) > len(b)):
        a, b = b, a
t = a

a = a[::-1]
if len(a) == 0:
    raise ValueError('a cannot be empty')
if len(b) == 0:
    raise ValueError('b cannot be empty')

convArray = []
# convArray.append(a[-1]*b[0])
for i in range(len(b)):
    convArray.append(dotProduct(a, b[i:i+len(a)]))

print "Convolved array -> ",convArray
print "Using np.convolve function -> ", np.convolve(t, b, 'full')
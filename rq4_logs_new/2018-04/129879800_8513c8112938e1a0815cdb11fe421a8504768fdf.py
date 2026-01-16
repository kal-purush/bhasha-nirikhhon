import numpy as np
import matplotlib.pyplot as plt
import scipy.io as sci
import math

#a
vals = sci.loadmat("Adot.mat")

X = vals['X']

#b1
theta = 3.141592653589793/3
V = np.matrix([[math.cos(theta), -math.sin(theta)], [math.sin(theta), math.cos(theta)]])

Y = np.matmul(V,X)

result = Y
plt.plot(np.array([result[0, i-1] for i in range(122)]), np.array([result[1,i-1] for i in range(122)]))
plt.plot(np.array([X[0, i-1] for i in range(122)]), np.array([X[1,i-1] for i in range(122)]))
plt.xlabel("X")
plt.ylabel("Y")
plt.show()

# The values are getting circled areound 120 degree

#b2

Z = np.matmul(V.transpose(),V)

print Z

# obviously the inverse of V multiplied with itself gives the neutral element


#c

D1 = np.matrix([[2, 0], [0,2]])
D2 = np.matrix([[2, 0], [0,1]])

#D1 is just duplicating the values
#D2 is taking the values and in the direction of X it is duplicating them while in the direction of Y the vlaues stay the same

#d

#goes wrong and i dont know why.
#what the convolution shoud do is take a field of data and discretely convolve it. So the "energy" of a system is getting added up to a certain point multiplied with the convoluton function.
#the convolution function here is a discrete array
A = np.convolve(V.transpose(),D1,V)
result = A


#plt.plot
plt.xlabel("X")
plt.ylabel("Y")
plt.show()


























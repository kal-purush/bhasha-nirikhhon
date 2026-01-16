#Physics91si_Lab6

import matplotlib.pyplot as plt 
import numpy as np 

#Useful functions

arr = np.eye(3) # creates n dimensional identity matrix

# np.linspace( , , ), np.logspace( , , )
arr = np.linspace(0, 20, 100) #Returns an array with 100 equally spaced pts between 0 and 20
arr = np.logspace(0, 20, 100) #Basically 10**(array from np.linspace())

def fib_scalar(n):
	if n<= 1:
		return n
	else:
		return fib_scalar(n-1) + fib_scalar(n-2)

print (fib_scalar(10))
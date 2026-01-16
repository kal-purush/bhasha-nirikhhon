import matplotlib.pyplot as plt 
import numpy as np 
from array import *
import operator
import cmath

print "Please tell if you want to calculate Fourier Transform for : \n 1. if you want to provide data points or, \n 2. a given function."
choice=int(raw_input())

if choice == 1 :

	print "Please enter the number of non-zero data points in x(n)."
	m2=int(raw_input())

	X = []
	for i in range(0,2) :
		X.append([])


	for i in range(0,m2):
		print "Enter the x-value of x(n) for data point number",i+1
		X[0].append(int(raw_input()))
		print "Enter the y-value of x(n) for data point number",i+1
		X[1].append(float(raw_input()))
	

	def y(w):
		p = 0
		for i in range(0,m2):
			p = p + X[1][i]*(np.cos(w*X[0][i])-1j*np.sin(w*X[0][i]))
		return p


	t = np.arange(-10,10, 0.01)
	x = y(t)

	print X
	c = np.arctan((x.imag)/(x.real))
	
	plt.plot(t,c)
	plt.ylim((-5,5))
	plt.show()

	plt.plot(t,abs(x))
	plt.show()


elif choice==2 :

	t = np.arange(-40,40,1)
	q = np.power(0.5,abs(t))
	r = np.sin(t)
	s = np.power(0.5,t)*((abs(t)+t)/2*t)

	m = t.size

	X = []
	for i in range(0,2) :
		X.append([])

	for i in range(1,m) :
		X[0].append(t[i])	

	for i in range(1,m):	
		X[1].append(q[i])

	plt.plot(X[0],X[1])
	plt.show()

	def y(w):
		p = 0
		for i in range(0,m-1):
			p = p + X[1][i]*(np.exp(-1j*w*X[0][i]))
		return p

	x = y(t)
	c = np.arctan((x.imag)/(x.real))

	print X

	plt.plot(t,c)
	plt.ylim((-5,5))
	plt.show()

	plt.plot(t,abs(x))
	plt.show()
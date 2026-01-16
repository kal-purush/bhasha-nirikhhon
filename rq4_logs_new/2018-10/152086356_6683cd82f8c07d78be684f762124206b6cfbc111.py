import matplotlib.pyplot as plt
import numpy as np
 
t = np.arange(-10,10, 0.01)
l = np.sin(2*t)
m = np.exp(t)

def n(t):
	if(t >=2 or t<=0): return 0
 	else: return 1

o = []
for i in range(len(t)):
	o.append(n(t[i]))	

"""Plotting the sinusoidal function.
plt.plot(t,l)
plt.ylabel('sin(2t)')
plt.show()

#Plotting the unit pulse function.
plt.plot(t,o)
plt.ylabel('Unit Pulse')
plt.show()

#Plotting the exponential function.
#plt.ylabel('exp(t)')
#plt.show()"""


print "\nA function can be checked for linearity and time-invariance by performing some checks.\nFor that, we need to have some user defined parameters like a, b and t0.\n"

a = input("Please enter the value of a : ")
a = int(a)

b = input("Please enter the value of b : ")
b = int(b)


t0 = input("Please enter the value of t0 : ")
t0 = int(t0)

def x1(t):
 if(t >=2 or t<=0): return 0
 else: return 1

def x2(t):
 if(t >=4 or t<=0): return 0
 else: return 1

'''
def x1(t):
  return np.square(t)

def x2(t):
  return np.sin(t)
'''
#Checking the linearity of the system y(t) = exp(-x(t)). If the two graphs plotted overlap, then the sytem is linear, else not.
def x3(t):
	return a*x1(t)+b*x2(t)

def y11(x):
	return np.exp(-x1(x))


def y12(x):
	return np.exp(-x2(x))


s11 = []
for i in range(len(t)):
	s11.append(a*y11(t[i])+b*y12(t[i]))	

plt.plot(t,s11)
#plt.show()

def y13(x):
	return np.exp(-x3(x))

s12 = []
for i in range(len(t)):
	s12.append(y13(t[i]))	

plt.plot(t,s12)
plt.ylabel('Linearity of exp(-x(t))')
plt.show() # Plotting both the graphs simultaneously.
		

#Checking the time invariance of the system y(t) = exp(-x(t)). If the two graphs plotted overlap, then the sytem is time invariant, else not.

def y14(x):
	return y11(x-t0)

def x4(t) :
	return x1(t-t0)

def y15(x):
	return np.exp(-x4(x))

s13 = []
for i in range(len(t)):
	s13.append(y14(t[i]))	

plt.plot(t,s13)

s14 = []
for i in range(len(t)):
	s14.append(y15(t[i]))	

plt.plot(t,s13)
plt.ylabel('Time invariance of exp(-x(t))')
plt.show()

#Checking the linearity of the system y(t) = x(2t). If the two graphs plotted overlap, then the sytem is linear, else not.
def y21(x):
	return x1(2*x)


def y22(x):
	return x2(2*x)


s21 = []
for i in range(len(t)):
	s21.append(a*y21(t[i])+b*y22(t[i]))	

plt.plot(t,s21)
#plt.show()

def y23(x):
	return x3(2*x)

s22 = []
for i in range(len(t)):
	s22.append(y23(t[i]))	

plt.plot(t,s22)
plt.ylabel('Linearilty of x(2t)')
plt.show()

#Checking the time invariance of the system y(t) = x(2t). If the two graphs plotted overlap, then the sytem is time invariant, else not.		
def y24(x):
	return y21(x-t0)

def y25(x):
	return x4(2*x)

s23 = []
for i in range(len(t)):
	s23.append(y24(t[i]))	

plt.plot(t,s23)

s24 = []
for i in range(len(t)):
	s24.append(y25(t[i]))	

plt.plot(t,s24)
plt.ylabel('Time invariance of x(2t)')
plt.show()


#Checking the linearity of the system y(t) = x(t-1)+x(-t-1). If the two graphs plotted overlap, then the sytem is linear, else not.
def y31(x):
	return x1(x-1)+x1(-x-1)


def y32(x):
	return x2(x-1)+x2(-x-1)


s31 = []
for i in range(len(t)):
	s31.append(a*y31(t[i])+b*y32(t[i]))	

plt.plot(t,s31)
#plt.show()

def y33(x):
	return x3(x-1)+x3(-x-1)

s32 = []
for i in range(len(t)):
	s32.append(y33(t[i]))	

plt.plot(t,s32)
plt.ylabel('Linearity of x(t-1) + x(-t-1)')
plt.show()
		
#Checking the time invariance of the system y(t) = x(t-1)+x(-t-1). If the two graphs plotted overlap, then the sytem is time invariant, else not.
def y34(x):
	return y31(x-t0)

def y35(x):
	return x4(x-1)+x4(-x-1)

s33 = []
for i in range(len(t)):
	s33.append(y34(t[i]))	

plt.plot(t,s33)

s34 = []
for i in range(len(t)):
	s34.append(y35(t[i]))	

plt.plot(t,s34)
plt.ylabel('Time invariance of x(t-1) + x(-t-1)')
plt.show()
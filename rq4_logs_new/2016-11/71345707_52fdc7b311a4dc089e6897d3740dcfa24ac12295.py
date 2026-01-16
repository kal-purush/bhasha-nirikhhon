#!/usr/bin/python3.4
# -*- coding: utf-8 -*-

#import matplotlib
import numpy as np
import matplotlib.pyplot as plt

def k1y(xi,yi,zi,g,h,a,eps):
	#print('in ky1')
	w = (1/a - g*h*(1.0/2/a + 1))
	k1 = 1.0
	k1_ = 0.1
	while ((k1 - k1_)> eps):
		k1_ = k1
		k1 = (1.0/2/a*yi + zi + ((yi+g*h*k1)+3)/(np.power(yi+g*h*k1,2)+2)/a) / w
	#print('end ky1',k1)
	return k1

def k1z(xi,yi,zi,g,h,a,eps):
	#print('in kz1')
	w = (1/a - g*h*(1.0/a + 1))
	k1 = 1.0
	k1_ = 0.1
	while ((k1 - k1_)> eps):
		k1_ = k1
		k1 = (1.0/a*yi + zi + np.power(np.power(xi+g*h,2)+(xi+g*h)+1,0.5)/a) / w
	#print('end kz1' , k1)
	return k1

def k2y(xi,yi,zi,k1,g,h,a,eps):
	#print('in ky2')
	w = (1/a - g*h*(1.0/2/a + 1))
	k2 = 1.0
	k2_ = 0.1
	while ((k2 - k2_)> eps):
		k2_ = k2
		k2 = (1.0/2/a*(yi+(1-2*g)*k1*h) + (zi+(1-2*g)*k1*h) + ((yi+(1-2*g)*k1*h+g*h*k2)+3)/(np.power(yi+(1-2*g)*k1*h+g*h*k2,2)+2)/a) / w
	#print('end ky2', k2)
	return k2

def k2z(xi,yi,zi,k1,g,h,a,eps):
	#print('in kz2')
	w = (1/a - g*h*(1.0/a + 1))
	k2 = 1.0
	k2_ = 0.1
	while ((k2 - k2_)> eps):
		k2_ = k2
		k2 = (1.0/a*(yi+(1-2*g)*k1*h) + (zi+(1-2*g)*k1*h) + np.power(np.power(xi+(1-g)*h,2)+(xi+(1-g)*h)+1,0.5)/a) / w
	#print('end kz2',k2)
	return k2

epsi = 0.0001
N = 100
A = -200
gamma = (3.0 + np.power(3,0.5)) / 6
#x = [(10.0/np.abs(A)*i) if (i <N) else (10.0/np.abs(A) + (1 - 10.0/np.abs(A))*(i-100)) for i in np.arange(N*2)]
#y = [0.0*i for i in np.arange(N*2)]
#z = [0.0*i for i in np.arange(N*2)]
x = [1.0/N*i for i in np.arange(N)]
y = [0.0*i for i in np.arange(N)]
z = [0.0*i for i in np.arange(N)]

y[0] = 0.0
z[0] = 5.0

print('begin for:')
for i in np.arange(N-1):
	#print('i = ',i)
	'''if i <100:
		h = 1/A/N
	else:
		h = (1 - 1/A)/N'''
	h = 1.0/N
	k1_y = k1y(x[i],y[i],z[i],gamma,h,A,epsi)
	k2_y = k2y(x[i],y[i],z[i],k1_y,gamma,h,A,epsi)
	y[i+1] = y[i] + h/2 * (k1_y + k2_y)
	k1_z = k1z(x[i],y[i],z[i],gamma,h,A,epsi)
	k2_z = k2z(x[i],y[i],z[i],k1_y,gamma,h,A,epsi)
	z[i+1] = z[i] + h/2 * (k1_z + k2_z)

plt.plot(x,y)
plt.plot(x,z)
plt.xlabel('x ось')
plt.ylabel('y,z ось')
plt.title('2 задача')
plt.grid()
#plt.savefig(file_)
plt.show()
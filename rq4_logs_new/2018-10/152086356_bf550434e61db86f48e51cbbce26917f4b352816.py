import matplotlib.pyplot as plt 
import numpy as np 
from array import *
import operator

print "Please enter the number of non-zero data points in h(n)."
m1=int(raw_input())

print "Please enter the number of non-zero data points in x(n)."
m2=int(raw_input())

H = []
for i in range(0,2) :
	H.append([])

X = []
for i in range(0,2) :
	X.append([])

nH1= []
for i in range(0,2) :
	nH1.append([])

for i in range(0,m1):
	print "Enter the x-value of h(n) for data point number",i+1
	H[0].append(int(raw_input()))
	print "Enter the y-value of h(n) for data point number",i+1
	H[1].append(float(raw_input()))

for i in range(0,m2):
	print "Enter the x-value of x(n) for data point number",i+1
	X[0].append(int(raw_input()))
	print "Enter the y-value of x(n) for data point number",i+1
	X[1].append(float(raw_input()))

for i in range(0,m1):
	nH1[0].append(-H[0][m1-i-1])
	nH1[1].append(H[1][m1-i-1])
	
Y = []
for i in range(0,2) :
	Y.append([])

check1 = 0
check2 = 0
shift1 = 0
shift2 = 1
count = 0

a = X[0][0]
b = X[0][m2-1]
a1 = nH1[0][0]
b1 = nH1[0][m1-1]

while(check1==0):
	c = a1+shift1
	d = b1+shift1
	if(c<=b):
		Y[0].append(shift1)
		Y[1].append(float(0))
		for i in range(0,m2):
			for j in range(0,m1):
				if (X[0][i] == nH1[0][j]+shift1):
					Y[1][count] = Y[1][count]+nH1[1][j]*X[1][i]
		count = count+1
		shift1 = shift1+1
	else:
		check1=1	

while(check2==0):
	c = a1-shift2
	d = b1-shift2
	if(d>=a):
		Y[0].append(-shift2)
		Y[1].append(float(0))
		for i in range(0,m2):
			for j in range(0,m1):
				if (X[0][i] == nH1[0][j]-shift2):
					Y[1][count] = Y[1][count]+nH1[1][j]*X[1][i]
		count = count+1
		shift2 = shift2+1
	else:
		check2=1	
		
print Y
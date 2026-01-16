'''
Linear perceptron in python , adapting from LinearPerceptron1.m(MATLAB)
With help from datacamp tutorial (Deep Learning with Python)
But based mostly on quoc v tutorial. Trying to keep the same mathematical notation
'''

import numpy as np
import random as random
import matplotlib.pyplot as plt
import math

'''
# Not good to declare tuple like this. Better use np.array
x0 = ((1,5),(1,4),(1.5,4),(2.5,3),(2.5,1.5),(2.5,2),(3.5,5),(3.5,4),(4.5,5),(4.5,4))
print (type(x0)) #class tuple
'''

x1=np.array([1, 1, 1.5, 2.5, 2.5, 2.5, 2.5, 3.5, 3.5, 4.5, 4.5])
x2=np.array([5, 4, 4, 3, 1.5, 2, 5, 5, 4, 5, 4])
x=np.zeros((len(x1),2))
x[:,0]=x1
x[:,1]=x2
y=np.array([0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1])

print('x is ')
print (x)
dimension = x.ndim #extract dimension/features of data 

#Array multiplication such as x1*x2 is equal to dot 
#product in python. No need to transpose.
#print ((x1*x2).sum()) 


#Initilization
deltaTheta = np.zeros((dimension))
gamma = np.zeros((dimension))
alpha = 0.1

#1 Initialize parameter theta and b at random
#Theta =  np.random.random((len(x1)))
Theta =  np.ones((dimension))
b = np.random.random()

for q in range(1000):

	#2 Pick a random training example
	i = np.random.randint(0,11) 

	#3 Compute partial derivatives for Theta1, Theta2... and b
	dotprod = ((Theta*x[i,:]).sum())+ b #equivalent of dot product theta'*x +b
	gamma = 1/(1+math.exp(-dotprod))
	deltaB = 2*(gamma-y[i])*(1-gamma)*gamma

	for n in range(2):
		#print (n)
		deltaTheta[n] = 2*(gamma-y[i])*(1-gamma)*gamma*x[i,n]

	
		#4 Update parameters
		Theta[n] = Theta[n] - alpha*deltaTheta[n]	
	b = b - alpha*deltaB
	
	print('Theta 0: ',Theta[0],'Theta 1: ',Theta[1])



xpredict = np.array([3,3])
dotprod =  ((Theta*xpredict).sum())+ b
gamma = 1/(1+math.exp(-dotprod))
print ('gamma predict is ',gamma)

if gamma > 0.5:
	ypredict = 1
else:
	ypredict = 0


x1mesh=np.arange(0,6,0.1)
x2mesh=np.arange(0,6,0.1)
xall=np.array([0 ,0])

gamma_all=np.zeros((len(x1mesh),len(x2mesh)))
print (gamma_all)
'''
for i in range (6):
	for j in range(6):
		gamma_all[i,j] = np.random.randint(0,5)
		

print (gamma_all)
'''



for i in range(len(x1mesh)):
	for j in range(len(x2mesh)):
		
		xall = [x1mesh[i],x2mesh[j]]		
		dotprodall = ((Theta *xall).sum()) + b  
		
		gamma_all[i,j] = 1/(1+math.exp(-dotprodall))



'''
for i in range(len(x1mesh)):
	for j in range(len(x2mesh)):
		
		xall = [i,j]		
		dotprodall = ((Theta *xall).sum()) + b  
		
		gamma_all[i,j] = 1/(1+math.exp(-dotprodall))

'''

		
print (gamma_all)
plt.imshow(gamma_all,origin='lower')
cbar=plt.colorbar()
plt.show()



'''
#This works
plt.plot(x[:,0],x[:,1],'^r')
plt.show()
'''


'''
#This works as well
fig, ax = plt.subplots()
ax.scatter(x[:,0],x[:,1])
plt.show()
'''
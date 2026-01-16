#!/usr/bin/env python3
# Copyright Douglas Gibbons
import numpy as np
from numpy import genfromtxt
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import random
import urllib


def sigmoid(z):
    return 1.0 /(1.0 + np.exp(-z))

def sigmoidDerivative(z):
  return sigmoid(z) * (1 - sigmoid(z));

def initWeights(numInputs,numOutputs):
    return 2 * np.random.random((numInputs,numOutputs)) - 1

def addOnes(arr):
    ones = np.ones((arr.shape[0],1))
    return  np.insert(arr,[0],ones,axis=1)

def addZeros(arr):
    zeros = np.zeros((arr.shape[0],1))
    return  np.insert(arr,[0],zeros,axis=1)

def convertOutputs(outputs):
    return (outputs > 0.5).astype(int)

# Prediction
def predict(X,thetas):
    
    activations       = []  # net inputs on each neuron per layer
    weighted_outputs  = [] # weighted outputs from each neuron per layer
    
    a = X # First set of activations are the inputs
    
    for theta in thetas:
        a_bias = addOnes(a)      # add bias terms to activations
        activations.append(a_bias)
        
        z = a_bias.dot(theta.T)  # add weights to each activation per neuron
        weighted_outputs.append(z)
        
        a = sigmoid(z)           # h(x) out of each neuron in the layer (also the activations for next layer)
        
    outputs = a # final outputs
    
    return (activations,weighted_outputs,outputs)
    

# Cost Function for Regularized Logistic Regression

# X is a 2D array of inputs for each sample
# ideal is a 2D array of ideal values for each sample
# lambd is the regularizaion parameter
# thetas is a list of 2d arrays of theta, starting at the "input -> first layer" theta
def cost(X,ideal,lambd,thetas):
    
    (activations,weighted_outputs,predicted) = predict(X,thetas)

    m = ideal.shape[0] # Numer of samples
    
    J = (1/m) *  sum(sum(  (-ideal * np.log(predicted) - (1 - ideal) * np.log(1-predicted)) ));
        
    # Sum all the theta values together, except the regularized terms
    sumThetas = 0
    for theta in thetas:
        sumThetas += sum(sum( theta[:,1:] * theta[:,1:] ))
        
    # Regularised cost function 
    r = (lambd/(2*m)) * (sumThetas);
    
    return J + r


# Backpropagation
def grads(X,Y,lambd,thetas):
    
    m = X.shape[0]
    Xb =  addOnes(X)
    deltas = []
    
    # Init the deltas
    for theta in thetas:
        deltas.append(np.zeros(theta.shape))
    
    (activations,weighted_outputs,outputs) = predict(X,thetas)
    
    # Backpropogation
    for t in range(0,m): # for every data row

        # Output layer
        deltaOut = np.array([(outputs[t] - Y[t,:])])
        deltas[-1] = deltas[-1] + deltaOut.T.dot(np.array([ activations[-1][t] ]))
        
        # Work backwards through hidden layers
        dLast = deltaOut
        for layer in range(len(deltas) -2,-1,-1):
            
            a = np.array([ activations[layer][t] ])
            z = np.array([ weighted_outputs[layer][t] ])
        
            d = (thetas[layer + 1].T.dot(dLast.T))[1:].T * sigmoidDerivative(z);
            deltas[layer] = deltas[layer] + d.T.dot(a)
            
            dLast = d
    
    return [ (1/m)*deltas[i] + (lambd/m) * addZeros(thetas[i][:,1:]) for i in range(0,len(deltas)) ]


# The Data
def ansToDigit(y):
    ans = (y > 0.5).astype(int)
    for i in range(10):
        if ans[i] == 1: return i
    return 0

# data description:
# https://archive.ics.uci.edu/ml/machine-learning-databases/semeion/semeion.names
dataSource = (
  'https://archive.ics.uci.edu/ml/machine-learning-databases/semeion/semeion.data'
)
data = np.loadtxt(urllib.request.urlopen(dataSource).readlines())
X = data[:,0:256].astype(int)
Y = data[:,256:].astype(int)
print("X data:",X.shape)
print("Y data:",Y.shape)


# Shuffle the values
XShuffled = []
YShuffled = []
indexes = np.random.permutation(len(Y))

for i in indexes:
    XShuffled.append(X[i])
    YShuffled.append(Y[i])

# Use most for dev and 100 for test
XDev  = np.array(XShuffled[0:-100])
YDev  = np.array(YShuffled[0:-100])
XTest = np.array(XShuffled[-100:])
YTest = np.array(YShuffled[-100:])


# System Initialization
np.random.seed(1)
hiddenLayer1Size = 30
hiddenLayer2Size = 20

theta1   = initWeights(hiddenLayer1Size,XDev.shape[1]+1)   # between input and hidden layer
theta2   = initWeights(hiddenLayer2Size,hiddenLayer1Size+1) # between hidden layer and output
theta3   = initWeights(Y.shape[1],hiddenLayer2Size+1) # between hidden layer and output
thetas   = [ theta1,theta2,theta3 ]


# The Learning Phase
lambd = 0.1
devCosts = []

testAccuracies = []

for i in range(0,500):
    J = cost(XDev,YDev,lambd,thetas)
    (theta1Grad,theta2Grad,theta3Grad) = grads(XDev,YDev,lambd,thetas)
    if i % 10 == 0: print('.', end="")
    
    theta1 = theta1 - theta1Grad
    theta2 = theta2 - theta2Grad
    theta3 = theta3 - theta3Grad
    
    thetas = [ theta1,theta2,theta3]
    devCosts.append(J)
      
    # Run test against test data
    (activations,weighted_outputs,outputs) = predict(XTest,thetas)
    answers = convertOutputs(outputs)
    right = wrong = 0
    
    for k in range(0,len(outputs)):
        if np.array_equal(np.array(YTest[k]),np.array(answers[k])): right +=1
        else: wrong +=1
    testAccuracies.append(right/(right+wrong))


# Plot of the cost against the number of iterations
plt.plot(devCosts)
plt.show()

# Plot of the accurancy on the test data against the number of iterations
plt.plot(testAccuracies)
plt.show()

# Prediction
m = XTest.shape[0]

right = 0
wrong = 0

for t in range(5):
    
    (activations,weighted_outputs,outputs) = predict(np.array([ XTest[t] ]),thetas)
    
    yDigit   = ansToDigit(YTest[t])
    ansDigit = ansToDigit(outputs[0])
    print("Actual:  ",yDigit)
    print("Guess:   ", ansDigit)

    np.array([ XTest[t] ])
    
    plt.imshow(np.array([ XTest[t] ]).reshape(16,16))
    plt.show()
    print("-------------------------------")
    
    if yDigit == ansDigit: right +=1
    else: wrong +=1

print("Right:",right)
print("Wrong:",wrong)
print("Accuracy: %i%%" % (right*100/(right+wrong)) )


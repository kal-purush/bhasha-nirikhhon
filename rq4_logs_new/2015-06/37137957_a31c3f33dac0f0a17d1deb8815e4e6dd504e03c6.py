'''
    Machine Learning on Coursera (Andrew Ng)
    Exercise 2.1: logistic regression

    use scipy optimization function package scipy.optimize, package fmin_bfgs to minimize 
    the function and get the opmitized papameter (theta) values
    
    data set needed: 'ex2data1.txt'
    
    @yingtang 

note: feature normalization is necessary

'''

import numpy as np
import matplotlib.pyplot as plt
import scipy.optimize as opt
import sys


def feature_norm(x):
    '''feature normalization'''
    global mu  #mean values of each feature
    global sigma  #standard deviation of each feature
    m,n = x.shape
    xnorm = np.ones((m,n))
    
    mu=[]
    sigma=[]
    
    '''note that the first column of X are all ones'''
    for i in range(1, n):
        mu.append(np.mean(x[:,i]))
        sigma.append(np.std(x[:,i]))
        xnorm[:,i]=(x[:,i]-mu[i-1])/sigma[i-1]

    return xnorm

def unnormalize_theta(theta):
    '''
    we obtained theata with the normailzed features

    now we keep the feature normalized and get back the unormalized values of theta
    '''
    n=len(mu)
    #unnormalize theta_0
    for i in range(n):
        theta[0] -= theta[i+1]*mu[i]/sigma[i]

    #unnormalize other thetas
    for i in range(n):
        theta[i+1] = theta[i+1]/sigma[i]
        
    return theta


def boundary_lines(theta):
    '''
    plot the boudary lines of the classification
    boundary is determined when sigmoid function is of value 0.5

    for the data with two features
    '''
    num=200 #number of points
    xpoints = np.linspace(20, 100, num)
    ypoints = -(theta[0]+theta[1]*xpoints)/theta[2]
    
    plt.figure(1)
    plt.plot(xpoints, ypoints)
    plt.ylim(20,110)
    plt.xlim(25,110)
    plt.show()
        
def sigmoid(x, theta):
    """compute the sigmoid function"""

    z = np.dot(x, theta) #dimesion = m x 1 

    sgmd =  1.0/(1.0+np.exp(-z)) 
    
    #return value is m x 1
    return sgmd

def cost_function(theta, x, y):
    '''cost function of the logistic regression'''
    #when h(x) is greater than 0.5
    m,n = x.shape

    J_pos = -np.dot(y.T, np.log(sigmoid(x, theta)))
    J_neg = -np.dot((1.0-y).T, np.log(1.0 - sigmoid(x, theta)))

    J = (J_pos + J_neg)*(1.0/m)

    return J
  
def gradient(theta,x,y):
    """compute the gradient of the cost function"""
    m, n = x.shape

    h = sigmoid(x, theta)
    h.shape=(m, 1)
    
    delta = h - y
  
    grad = (1.0/m)*np.dot(x.T, delta).flatten()

    return grad

                  
def visualization(x, y):
    "visualize the original data"
    admitted = np.where(y==1)
    rejected = np.where(y==0)

    plt.scatter(x[admitted, 1], x[admitted, 2], marker='o', c='y', s=25, label='admitted')
    plt.scatter(x[rejected, 1], x[rejected, 2], marker='+', c='k', s=25, label='rejected')
    plt.xlabel("Exam 1 score")
    plt.ylabel("Exam 2 score")
    plt.legend()
   # plt.show()

def prediction(x, theta):
    p = sigmoid(x, theta)
    print "The admission probability is: ", p
    if p>=0.5:
        y=1
    else:
        y=0

    return y
    
    

##############################################
#            Main Program                    #
##############################################

dat = np.loadtxt("ex2data1.txt", delimiter=',')

#get the number of rows m and number of columns n
m,n = dat.shape

#get x data
xdata = np.ones((m,n)) # m x n array
xdata[:,1:] = dat[:,0:2] # 
#get y data
ydata = dat[:,2]  #m x 1
ydata.shape=(m, 1)

init_theta = np.zeros((n, 1)) # n x 1 

#check what data looks like call "visualization"
visualization(xdata, ydata)

#normalize the features
xdata = feature_norm(xdata)

#check if the cost function is correct, should be 0.693
print "initial cost function value:", cost_function(init_theta, xdata, ydata)

#minimize the theta sets using scipy fmin_bfgs algorithm 
theta_min, j_min = opt.fmin_bfgs(cost_function, init_theta, fprime=gradient, args=(xdata, ydata), full_output=True, disp=False)[0:2] 

print "final theta value:", theta_min
print "the minimum cost function value:", j_min

#unnormalize theta values, not scalable in this case
un_theta = unnormalize_theta(theta_min)

#plot the boundary lines
boundary_lines(un_theta)


#Test your program, should report 0.776
xtest=[1.0, 45.0, 85.0]
xtest=np.array(xtest)
result=prediction(xtest, un_theta)

print("(1: admitted, 0: not admitted) The student will be: %01d"  %result)
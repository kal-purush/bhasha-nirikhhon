"""This document builds a fixed point interation algorithm for solving an equation
g(x*) = x*. The solution for one particular g, namely g(x) e^(-x), is graphed, where
approximations made are shown in plus's. The cyan and magenta plus's converge as g(x)
approaches x."""

import numpy as np
import matplotlib.pyplot as plt

def dApprox(f,x0):
    h = 10**-8
    return (1/(2*h))*(f(x0+h)-f(x0-h))

def fixedPointMethod(g,x0,a,b):

    #come back to args
    #What's the idea here?
        #We want to find the root of a function f
            #This means solving f(alpha) = 0
        #The fixed point method entails reformatting the problem to finding
        #the fixed point of a function g
            #this is to say, we want to solve g(alpha) = alpha where g is a function
            #that satisfies g(alpha) = alpha iff f(alpha) = 0
                #This may be as simple as taking g(x) = f(x) + x
    if not ((g(a) < a and g(b) > b) or (g(a) > a and g(b) < b)):
        #This tests to see if g has a fixed point alpha
        #If g(a) < a & g(b) > b, there is alpha in [a,b] such that g(alpha) = alpha
        #So if not that, quit the program
        print("This function does not have a fixed point on the interval ["+str(a)+","+str(b)+"]")
        return
    delta = (b-a)/100
    t = np.arange(a,b+delta,delta)
    dgt = dApprox(g,t)
    for i in dgt:
        if abs(i) >= 1:
               print("This function does not contract on the interval ["+str(a)+","+str(b)+"] -- fixed point interartion will fail")
               return
    else:
        #If we do have a fixed point and the function does contract, continue
        atol = 10**-8
        xn = [x0]
        rhon = [1]
        xn.append(g(x0))
        print(xn)
        curr = abs(xn[1] - xn[0])
        while curr > atol:
            k = len(xn)-1
            #k is the largest index of list xn
            #stars at k = 1
            xn.append(g(xn[k]))
            #now largest index of list xn is k+1
            curr = abs(xn[k+1] - xn[k])
        n = len(xn)
        alpha = xn[n-1]
        rho1 = abs(dApprox(g,alpha))
        rho2 = (abs(xn[n-2] - alpha)/abs(x0 - alpha))**(1/(n-1))
    xn = np.array(xn)
    plt.plot(t,t,'g',t,g(t),'b')
    plt.plot(xn,g(xn),'m+',g(xn),g(xn),'c+')
    plt.show()
    print(rho1,rho2)
    return alpha,n

def g(x):
    return np.e**(-x)



fixedPointMethod(g,1,0,1.4)
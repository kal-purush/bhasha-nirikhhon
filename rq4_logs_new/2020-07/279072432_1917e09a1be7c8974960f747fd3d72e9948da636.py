"""This document runs the bisection method on a function, f, and a root, x*"""

import numpy as np
import matplotlib.pyplot as plt

###Bisection method
#Inputs: function f, interval [a,b], tolerance level atol.
#Outputs: a value p within atol of the true root x*, the number of steps required n.

def bisectMethod(f,a,b,atol):
    #If f(a)*f(b) < 0, IVT guarantees root f(x*) = 0.
    #We check whether f(a)f(p) < 0 or f(p)f(b) < 0, where p is midpoint.
    #If we get a negative, we take the midpoint of [a,p] or [p,b] and repeat.
    #We stop the algorithm when |x_n - x_n-1| < atol.
    fa = f(a)
    fb = f(b)
    if (a >= b) or (fa*fb >= 0) or (atol <= 0):
        print("Hrm... I don't like the input... I'm quitting")
        return
    n = int(np.ceil((np.log2(b-a)) - (np.log2(2*atol))))
    eps = 1.1*(10**-16)
    print(n)
    pvals = []
    i = 1
    while i < 2*n:
        print('while')
        p = (a + b)/2
        fp = f(p)
        pvals.append(p)
        #print(i,p)
        if abs(fp) < eps:
            print('We found a root ---> p = '+str(p))
        elif i >= 2 and abs(pvals[i-1] - pvals[i-2]) < atol:
            print('We are within tolerance')
            print('We took '+str(i)+' steps to find our solution '+str(p))
            i = 2*n
        elif fa*fp < 0:
            #print('first half')
            b = p
            fb = fp
        else:
            #print('second half')
            a = p
            fa = fp
        i+=1
    #Let's get in the habit of commenting :).
    #I believe at the end of n interations, we are left with our p.
    #This is the IVT *and* the Cauchy Criterion in action :O
    t = np.arange(0,25,.1)
    z1 = np.zeros(len(t))
    pvals = np.array(pvals)
    z2 = np.zeros(len(pvals))
    tt = f(t)
    plt.xlabel('x')
    plt.ylabel('f(x)')
    plt.plot(t,tt,'g',t,z1)
    plt.plot(pvals,z2,'r+')
    plt.plot(pvals[len(pvals)-1],0,'c+')
    plt.show()
    
    return p

atol = 10**-8
def goodFunc(x):
    output = x**3 - 30*(x**2) + 2552
    return output
print(goodFunc(2))

p = bisectMethod(goodFunc,0,25,atol)

    
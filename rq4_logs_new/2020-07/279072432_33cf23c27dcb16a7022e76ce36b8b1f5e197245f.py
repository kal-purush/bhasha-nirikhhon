"""This file computes two versions of the same function, namely
sqrt(x+1) - sqrt(x) VS. 1/(sqrt(x) + sqrt(x+1)), and shows how
the relative error between shrinks as x grows logarithmically."""

import numpy as np
import matplotlib.pyplot as plt

def testCancelErr():
    fx1 = []
    fx2 = []
    t = []
    absErr = []
    relErr = []
    for i in range(20):
        x = 10**i
        curr1 = np.sqrt(x+1) - np.sqrt(x)
        curr2 = 1/(np.sqrt(x+1) + np.sqrt(x))
        fx1.append(curr1)
        fx2.append(curr2)
        t.append(x)
        absErr.append(abs(curr1 - curr2))
        relErr.append(absErr[i]/abs(curr2))
    fx1 = np.array(fx1)
    fx2 = np.array(fx2)
    t = np.array(t)
    absErr = np.array(absErr)
    relErr = np.array(relErr)
    print(fx1)
    print(fx2)
    plt.xscale('log')
    plt.yscale('log')
    plt.plot(t,fx1,'g',t,fx2,'b+',t,absErr,'r--',t,relErr,'y--')
    plt.show()
    for i in range(20):
        print(i,"|",t[i],"|",fx1[i],"|",fx2[i],"|",absErr[i],"|",relErr[i])
testCancelErr()
        
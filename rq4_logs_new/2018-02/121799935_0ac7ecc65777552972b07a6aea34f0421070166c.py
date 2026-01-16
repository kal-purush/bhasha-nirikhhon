#Bhavinkumar Patel
#Akashkumar Patel


import math
import numpy as np


def f1(x):
    return x**3 - 2*x - 5
def f2(x):
    return (math.e ** (-1*x)) - x
def f3(x):
    return x * math.sin(x) - 1
def f4(x):
    return (x ** 3) - 3* (x ** 2) + 3*x -1


def bisection(f , a ,b , tolerance , iteration):
    print "iteration   a        b        solution       F(c)      Tolerance"
    c = a
    x7 = c + 1.0 * tolerance
    x6 = c + 1.0 * tolerance
    for i in range(1,iteration):
        x5 = x7
        x7 = x6
        x6 = c
        
        c = (a+b)/2
        computedTolerance = (b-a)/2
        fc = f(c)
        fa = f(a)
        fb = f(b)
        if (f(a) * f(b)) > 0:
            print "Error:SameSign"
            break
        print '%3d %11f %11f %11f %11f %11f' %(i,a,b,c,fc,computedTolerance)
        if fc == 0:
            return c
        if abs(computedTolerance) < tolerance:
            break
        if f(a) * f(c) < 0:
            b = c
        else:
            a = c
            
    rate  = math.log( abs((c - x6) / (x6 - x7)) ) / \
            math.log( abs((x6 - x7) / (x7 - x5)) )
    print "Approximate Convergence Rate.:",rate

def bisection1(f , a ,b , tolerance , iteration):
    #print "iteration   a        b        solution       F(c)      Tolerance"
    for i in range(1,iteration):
        c = (a+b)/2
        computedTolerance = (b-a)/2
        fc = f(c)
        fa = f(a)
        fb = f(b)
        if (f(a) * f(b)) > 0:
            print "Error:SameSign"
            break
        #print '%3d %11f %11f %11f %11f %11f' %(i,a,b,c,fc,computedTolerance)
        if fc == 0:
            return c
        if abs(computedTolerance) < tolerance:
            return c

        if f(a) * f(c) < 0:
            b = c
        else:
            a = c
            

print "---------------------------Bisection-------------------------"
print "------------------------Tolerance limit = 0.0001 -------------"
print "\n"

print "--------  function a ---------"
print "---------- a = 2 , b = 3 ---------"
print "Root of function a:",bisection1(f1,2.0,3.0,0.0001,30)
bisection(f1,2.0,3.0,0.001,30)
print "\n"

print "--------  function b ---------"
print "---------- a = 0 , b = 1--------"
print "Root of function b:",bisection1(f2,0.0,1.0,0.0001,30)
bisection(f2,0.0,1.0,0.0001,30)
print "\n"

print "--------  function c ---------"
print "---------- a = 31 , b = 32 ---------"
print "Root of function c:",bisection1(f3,31.0,32.0,0.0001,30)
bisection(f3,31.0,32.0,0.001,30)
print "\n"

print "--------  function d ---------"
print "---------- a = 0 , b = 2 --------"
print "Root of function d:",bisection1(f4,0.0,2.0,0.0001,30)
bisection(f4,0.0,2.0,0.001,30)
rate = 1.0
print "Approximate Convergence Rate.:",rate

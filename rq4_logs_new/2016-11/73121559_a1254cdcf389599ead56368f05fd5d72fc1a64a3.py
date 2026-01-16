#!/bin/python

import sys

n,m = raw_input().strip().split(' ')
n,m = [int(n),int(m)]

if n <=2 and m <=2:
    print 1
    exit()

x=False
y=False

if n%2==0:
    x=True
if m%2==0:
    y=True

if x and y:
    print (n*m)/4
    exit()

if (x and not y ) or (y and not x):
    mfixed=0
    nfixed=0
    total=0
    
    if not y:
        mfixed=m-1
        total=((mfixed*n)/4)+((n*1)/2)
    if not x:
        nfixed=n-1
        total=((m*nfixed)/4)+(m*1/2)
    print total
    exit()
    
if not x and not y:
    mfixed=m-1
    nfixed=n-1
    
    total=((mfixed*nfixed)/4)+((m/2)+(n/2)+1)
    print total
    exit()
            
        
    
    
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 21:36:24 2018

@author: Freek
"""

import time
num_dict = dict()
temp = []

def odd_or_even(n):
    counter = 1
    
    while n>1:
        # Check if n is in dictionary
        if n in num_dict:
            counter += num_dict[n]-1
            break
        # If n is even
        if n%2 == 0:
            n = int(n/2)
            counter += 1            
        # If n is odd
        else:
            n = int(3*n+1)
            counter += 1
    return counter

st = time.time()
for n in range(1,int(1e+6)):
    x = odd_or_even(n)
    temp.append(x)
    # Optimisation: for numbers n 
    if n < int(5e+5):
        num_dict[n] = x
print('%s seconds' %round(time.time()-st,1))
print('longest chain: %d' %max(temp))
        
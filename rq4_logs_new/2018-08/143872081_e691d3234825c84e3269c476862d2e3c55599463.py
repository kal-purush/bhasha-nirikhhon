# -*- coding: utf-8 -*-
"""
Created on Sat Aug 18 09:45:47 2018

@author: Freek
"""

def square_digits(n):
    x = str(n)
    return sum([int(i)**2 for i in x])
    
total_1 = 0
total_89 = 0
set_89 = set()

for i in range(2,int(1e+07)+1):
#for i in range(2,100):
    #print('start:', i)
    x = i
    while x!= 1 and x!= 89:
        x = square_digits(x)
        if x in set_89: x=89
        #print(i)
        
    if x == 89:
        total_89 += 1
        if i < 600: set_89.add(i)

    if x == 1:
        total_1 += 1
        
print('Total 89: %d' %total_89)
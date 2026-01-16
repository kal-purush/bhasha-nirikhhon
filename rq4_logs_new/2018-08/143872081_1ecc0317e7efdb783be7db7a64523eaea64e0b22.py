# -*- coding: utf-8 -*-
"""
Created on Mon Aug 20 07:32:42 2018

@author: EL11LV
"""

from functools import reduce
#from math import factorial as fact

factorials = []
for i in range(0,10):
    if i == 0:
        factorials.append(1)
    else:
        factorials.append(reduce(lambda x,y: x*y, range(1,i+1)))

temp = []
temp3 = []
for i in range(1,50000):
    x = str(i)
    temp.append(int(x))
    temp2 = []
    for j in x:
        temp2.append(factorials[int(j)])
#       if j == '0':
#          temp2.append(1)
#       else:
#          temp2.append(reduce(lambda x,y: x*y, range(1,int(j)+1)))
    temp3.append(sum(temp2))
    
#print(set(temp) & set(temp3))
equal_sum = len([i for i,j in zip(temp,temp3) if i==j])-2

print('Sum of all numbers that are equal to the factorial of their digits: %d' %equal_sum)
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 17 23:24:08 2018

@author: Freek
"""

def Triangle(n):
    return int(n*(n+1)/2)

def Pentagonal(n):
    return int(n*(3*n-1)/2)

def Hexagonal(n):
    return int(n*(2*n-1))

temp_Triangle = []
temp_Pentagonal = []
temp_Hexagonal = []
for i in range(1,int(8e+04)):
    temp_Triangle.append(Triangle(i))
    temp_Pentagonal.append(Pentagonal(i))
    temp_Hexagonal.append(Hexagonal(i))
    
for i in temp_Triangle:
    if i in temp_Pentagonal and i in temp_Hexagonal:
        print('T%d = P%d = H%d = %d' %(int(temp_Triangle.index(i)+1),int(temp_Pentagonal.index(i)+1),int(temp_Hexagonal.index(i)+1),i))
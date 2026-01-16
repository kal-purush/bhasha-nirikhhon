# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 18:10:22 2018

@author: Freek
"""

set_palindrome = set()

def palindrome(n):
    x = n
    counter = 0
    test = 'N'
    while counter<50 and test == 'N' :        
        counter+=1
        x = x + int(str(x)[::-1])
        if str(x) == str(x)[::-1]:
            test = 'Y'
        else:
            test = 'N'
    return counter, n, test

temp = []
for i in range(196,10001):
    _, n, test = palindrome(i)
    if test == 'N':
        temp.append(n)
        
print('There are %d Lychrel numbers below ten-thousand' %len(temp))
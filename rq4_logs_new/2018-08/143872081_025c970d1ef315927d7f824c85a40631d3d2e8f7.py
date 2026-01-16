# -*- coding: utf-8 -*-
"""
Created on Sat Aug 25 14:17:22 2018

@author: Freek
"""

def numbers_to_ten(n):
    word = ''
    if n == 1:
        word = 'one'
    if n == 2:
        word = 'two'
    if n == 3:
        word = 'three'
    if n == 4:
        word = 'four'
    if n == 5:
        word = 'five'
    if n == 6:
        word = 'six'
    if n == 7:
        word = 'seven'
    if n == 8:
        word = 'eight'
    if n == 9:
        word = 'nine'
    return word
    
def numbers_to_twenty(n):
    word = ''
    if n == 10:
        word = 'ten'
    if n == 11:
        word = 'eleven'
    if n == 12:
        word = 'twelve'
    if n == 13:
        word = 'thirteen'
    if n == 14:
        word = 'fourteen'
    if n == 15:
        word = 'fifteen'
    if n == 16:
        word = 'sixteen'
    if n == 17:
        word = 'seventeen'
    if n == 18:
        word = 'eighteen'
    if n == 19:
        word = 'nineteen'
    return word

def numbers_ten(n):
    word = ''
    if n == 20:
        word = 'twenty'
    if n == 30:
        word = 'thirty'
    if n == 40:
        word = 'forty'
    if n == 50:
        word = 'fifty'
    if n == 60:
        word = 'sixty'
    if n == 70:
        word = 'seventy'
    if n == 80:
        word = 'eighty'
    if n == 90:
        word = 'ninety'
    return word
        
def numbers_hundred(n):
    word = ''
    if n == 100:
        word = 'onehundred'
    if n == 200:
        word = 'twohundred'
    if n == 300:
        word = 'threehundred'
    if n == 400:
        word = 'fourhundred'
    if n == 500:
        word = 'fivehundred'
    if n == 600:
        word = 'sixhundred'
    if n == 700:
        word = 'sevenhundred'
    if n == 800:
        word = 'eighthundred'
    if n == 900:
        word = 'ninehundred'
    return word


def below_hundred(n):
    x = 0
    n_str = str(n)
    l = len(n_str)
    if l == 1:
        x = numbers_to_ten(n)
    elif l == 2:
        if n>=10 and n<20:
            x = numbers_to_twenty(n)
        if n>=20:
            if n_str[1]=='0':
                x = numbers_ten(n)  
            else:
                x = numbers_ten(int(n_str[0]+'0')) + numbers_to_ten(int(n_str[1]))
    return x

def numbers_to_words(n):
    x = 0
    n_str = str(n)
    l = len(n_str)
    if l < 3:
        x = below_hundred(n)
    elif l == 3:
        if n_str[1:]=='00':
            x = numbers_hundred(n)
        else:
            x = numbers_hundred(int(n_str[0]+'00')) + 'and' + below_hundred(int(n_str[1:]))
    else :
        x = 'onethousand'
    return x

temp = []
for i in range(1,1001):
    temp.append(len(numbers_to_words(i)))
    
print('Sum of letters of all numbers below 1000: %d' %sum(temp))  # 21,124
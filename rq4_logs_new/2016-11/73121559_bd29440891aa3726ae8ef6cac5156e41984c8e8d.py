#!/bin/python

import sys


n = int(raw_input().strip())

#print n
divs=[]

for i in range(1,n+1):
    if n%i == 0:
        divs.append(str(n/i))

print divs
#champion=[number,sum]
champion=[0,0]

for number in divs:
    tmp=0
    for digit in number:
        tmp+=int(digit)
    #print tmp    
    if tmp > champion[1]:
        champion[0]=number
        champion[1]=tmp
        #print "champion",champion
    if tmp == champion[1]:
        if number < champion[0]:
            champion[0]=number
            champion[1]=tmp
                        
print champion[0]
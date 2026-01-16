# -*- coding: utf-8 -*-
"""
Created on Fri Aug 17 07:45:02 2018

@author: Freek
"""

#number = 220
#print(number)

temp3 = []
max_numb = 10000

# 220
for number in range(2,max_numb+1):
    
    #     
    temp = []
    temp.append(1)
    for i in range(2,int(number/2)+1):
        if number % i == 0:
            temp.append(i)
            
    #print('t:', sum(temp))  # 284
    
    number2 = sum(temp)  # 284     
    temp2 = []
    temp2.append(1)
    for i in range(2,int(number2/2)+1):
        if number2 % i == 0:
            temp2.append(i)
    
    #print('t2:',sum(temp2))  # 220
    
    if sum(temp2)==number and number!=number2: # if 220 == 220
        temp3.append(number) # 220
        temp3.append(number2) # 284
        print('n:', number) # 220
        print('n2:', number2) # 284
        print('t3:', sum(temp3)) # 504

print('Sum of proper divisors below %d is %d' %(max_numb,sum(temp3)/2)) # double pairs are registered
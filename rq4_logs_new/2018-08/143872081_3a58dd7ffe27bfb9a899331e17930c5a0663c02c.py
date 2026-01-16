# -*- coding: utf-8 -*-
"""
Created on Wed Aug 29 17:15:25 2018

@author: Freek
"""

from itertools import permutations, product

#PETE: 9 four-sided dices
x_PETE = [1,2,3,4]
r_PETE = 9

#COLIN: 6 six-sided dices
x_COLIN = [1,2,3,4,5,6]
r_COLIN = 6

PETE_set = dict() # 9 to 36
for i in range(min(r_COLIN,r_PETE),max(r_COLIN*len(x_COLIN)+1,r_PETE*len(x_PETE)+1)):
    PETE_set[i] = 0

temp = [p for p in product(x_PETE,repeat=r_PETE)]
for i in temp:
    k = sum(i)
    PETE_set[k] += 1
  
COLIN_set = dict() # 6 to 36
for i in range(min(r_COLIN,r_PETE),max(r_COLIN*len(x_COLIN)+1,r_PETE*len(x_PETE)+1)):
    COLIN_set[i] = 0

temp = [p for p in product(x_COLIN,repeat=r_COLIN)]
for i in temp:
    k = sum(i)
    COLIN_set[k] += 1
    
denom = (len(x_PETE)**r_PETE) * (len(x_COLIN)**r_COLIN)
nom = 0

for i in range(r_COLIN,min(r_COLIN*len(x_COLIN)+1,r_PETE*len(x_PETE)+1)):
    for j in range(i+1,min(r_COLIN*len(x_COLIN)+1,r_PETE*len(x_PETE)+1)):
        nom += PETE_set[j] * COLIN_set[i]


# Pete: 1+2+3 = 6
# Colin: 4+4+3+2+1 = 14 
# draw: 4

print('Probability that Pete defeats Colin is %f' %round(nom/denom,7))
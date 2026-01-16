'''
Problem:
If we list all the natural numbers below 10 that are multiples of 3 or 5, we get 3, 5, 6 and 9. The sum of these multiples is 23.
Find the sum of all the multiples of 3 or 5 below 1000.

Answer:
233168
Best time:
0.000s

Walkthrough:
use the summation formule: n(n+1)/2 for 3 and 5
remove multiple of 15
'''
import datetime

# the simplest, but slowest method
# 5.656s at 10000000
def findSum(limit):
    s = 0
    for i in range(limit):
        if(i % 3 == 0 or i % 5 == 0):
            s += i
    return s

# take all the 3s and then check if the 5s aren't multiple of 3 (saves time because less comparisons)
# 1.531s at 10000000
def findSum2(limit):
    s = 0
    for i in range(0, limit, 3):
        s += i
        
    for i in range(0, limit, 5):     
        if(i % 3 != 0):
            s += i
    return s

#use the sum function instead of a loop
# 0.947s at 10000000
# 9.640 at 100000000
def findSum3(limit):
    n = (limit - 1)//3
    s = 3/2 * n * (n + 1)
       
    for i in range(0, limit, 5):     
        if(i % 3 != 0):
            s += i
            
    return s

#use all the sum function, remove multiple of 3 and 5 (15)
# 0s at 10000000
# 0s at 100000000
# 0s at 1000000000000
# 0.002s at 1000000000000000000
def findSum4(limit):
    n3 = (limit - 1)//3
    n5 = (limit - 1)//5
    n15 = (limit - 1)//15

    s = (3/2 * n3 * (n3 + 1)) + (5/2 * n5 * (n5 + 1)) - (15/2 * n15 * (n15 + 1))
       
    return s

number = 1000
print(findSum4(number))
'''
Given an integer, N, 
traverse its digits (d1,d2,...,dn) and determine how many 
digits evenly divide  N(i.e.: count the number of times  divided by each digit i 
has a remainder of ). Print the number of evenly divisible digits.

Note: Each digit is considered to be unique, so each occurrence of the same evenly divisible digit 
should be counted (i.e.: for N=111, the answer is 3 ).

Input Format

The first line is an integer, T, indicating the number of test cases. 
The T subsequent lines each contain an integer, N.

Sample Input

2
12
1012

Sample Output

2
3

'''

import sys

def countDivisors(n,originalN) :
    if n == 0 :
        return 0
    if n%10 != 0 :    
        if originalN%(n%10) == 0 :
            return 1 + countDivisors(n/10,originalN)
        
    return countDivisors(n/10,originalN)
    

t = int(raw_input().strip())
for a0 in xrange(t):
    n = int(raw_input().strip())
    print countDivisors(n,n)
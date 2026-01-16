'''
Author: Trinity Lundgren
Date: 2019-10-09
Description: Takes as inputs two binary integers at the command line separated
by spaces. Returns the sum of the binary integers as a list, with the 0th 
element representing 2^0, and so on.
'''

import sys

def binary_to_decimal(binaryList):
    decimal = 0
    for element in enumerate(binaryList):
        decimal += element[1]*2**element[0]
    return decimal

def binary_add(a, b):

    # Convert string binary numbers to lists of equal length
    n = max(len(a), len(b))
    a = list(map(int, a.zfill(n)))
    b = list(map(int, b.zfill(n)))
    a.reverse()
    b.reverse()

    c = [0] * (n + 1)
    carry = 0
    for i in range(n):
        small_sum = a[i] + b[i] + carry
        if small_sum < 2:
            c[i] = small_sum
            carry = 0
        else:
            c[i] = 0
            carry = 1
    c[n] = carry

    print(a, " + ", b, " = ", c)
    print("In decimal, ", binary_to_decimal(a), " + ", binary_to_decimal(b),
          " = ", binary_to_decimal(c))
    return c

a = sys.argv[1]
b = sys.argv[2]

binary_add(a, b)




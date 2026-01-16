#!/usr/bin/python3
"""
Contains the Pascal triangle implementation
along with helper functions
From the binomial expansion of (a + b)**n, to get the coefficients of terms we use nCr:
These coefficients correspond to the numbers in the Pascal Triangle at row n. 
"""

def factorial(n):
"""
Returns the factorial of a given number
Needed in the implementation of the nCr function
"""
    result = 1
    if n == 0 and n == 1:
	    return 1
    if n < 0:
	    raise ValueError("No factorial of a negative number")

    for i in range(1, n+1):
	    result = result * i
	
    return result

def nCr(n, r):
  """helper function for pascal_triangle
  Computes nCr which is useful in creating the triangle
"""
    return int(factorial(n) / (factorial(r)* factorial(n-r))) 

def pascal_triangle(n):
  """Creates the pascal triangle with n rows
  """
    for i in range(j):
        print(list((nCr(i, j) for j in range(0, i+1))))
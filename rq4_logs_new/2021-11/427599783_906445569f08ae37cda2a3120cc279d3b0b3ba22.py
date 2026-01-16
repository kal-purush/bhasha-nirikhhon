"""
Given a two-dimensional matrix of integers matrix, 
determine whether it's a Toeplitz matrix. 
A Toeplitz is one where every diagonal descending 
from left to right has the same value.
"""

def check_Toeplitz(matrix):
  num_rows = len(matrix)
  if num_rows < 2:
    return None
  for i in range(num_rows-1):
    row_a = matrix[i]
    row_b = matrix[i+1]
    for element_a, element_b in zip(row_a[:-1], row_b[1:]):
      if element_a != element_b:
        return False
  return True

matrix = [
    [0, 1, 2],
    [3, 0, 1],
    [4, 3, 0],
    [5, , 3]
]
res = check_Toeplitz(matrix)
print(res)
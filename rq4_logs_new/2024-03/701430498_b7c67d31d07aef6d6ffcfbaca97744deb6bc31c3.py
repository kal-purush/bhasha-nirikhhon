
"""Вариант 27.
1. В матрице найти среднее арифметическое положительных элементов, кратных 3.
"""

import numpy as np
import random
def Main():
   matrix = [[random.randint(-100,100) for x in range(3)] for x in range(3)]
   print(matrix)
   elem = [row for row in matrix if any( i > 0 and i % 3 == 0 for i in row)]

   rslt = np.mean(elem)
   print(rslt)

if __name__ == '__main__':
    Main()
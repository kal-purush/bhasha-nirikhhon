#Square root of only positive number

import math
number = int(input())

#Checking if the number is positive number
if number<0:
  print("Please give a Positive number...")
else:
  squareRoot = math.sqrt(number)
  print(squareRoot)
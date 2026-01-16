# numpy enriches python by bringing super fast multi-dimensional arrays
# for faster processing of large data, because they take up less memory
# and also support various arithmetic operations that regular python lists dont

import numpy as np
import random

array = np.array([1, 2, 3])
print(f"type of this array = {array} is {type(array)}")

# matrix
m_array = np.array([[10, 20, 30], [40, 50, 60]])
print("multi-dimensional array, matrix: \n", m_array)
print("shape(tuple that tell the number of items in each dimension) of this array = ", m_array.shape)

# to initialize an array with specific number
zero_m_default = np.zeros((3, 4))
zero_m_int = np.zeros((3, 4), dtype=int)
one_m_default = np.ones((3, 4))
one_m_int = np.ones((3, 4), dtype=int)
fill_num_m = np.full((3, 4), 5, dtype=int)

print("zero initialized matrix (default): \n", zero_m_default)
print("zero initialized matrix (int): \n", zero_m_int)
print("one initialized matrix (default): \n", one_m_default)
print("one initialized matrix (int): \n", one_m_int)
print("any num initialized matrix (int): \n", fill_num_m)

random_m = np.random.random((3, 4))
print("random num initialized matrix : \n", random_m)
print("num at [0,0]: ", random_m[0, 0])
print("num at [0,1]: ", random_m[0, 1])

# this is somethng very porwerful, we can check a condition fro individual item in an array
# using simple code of line below, and it would return a matrix of boolean values
# corresponding to the ondition we checked
print(random_m > 0.5)

# another powerful operation
# using below code of line, we can return an array of items in array that follow a condition
print(random_m[random_m > 0.5])

# sum of all items in an array
print(np.sum(random_m))

# floor of each item in an array
print(np.floor(random_m))

# ceil of each item in an array
print(np.ceil(random_m))

# round of each item in an array
print(np.round(random_m))

# arithmetic operation btw numbers and arrays
# all arithmetic operation are supported by numpy arrays

first = np.array([1, 2, 3])
second = np.array([1, 2, 3])
print("first = ", first)
print("second = ", second)
print("first + second = ", first + second)
print("first - second = ", first - second)
print("first * second = ", first * second)
print("first / second = ", first / second)
print("first // second = ", first // second)

print("\narithmetic operations btw num and array: ")
print("first + 2 = ", first + 2)
print("first - 2 = ", first - 2)
print("first * 2 = ", first * 2)
print("first / 2 = ", first / 2)
print("first // 2 = ", first // 2)

# real world application of above case is if we have an array of inch n we want to convert to cm

dimensions_inch = np.array([1, 2, 3])
dimensions_cm = dimensions_inch * 2.54
print(
    f"converting inch matrix to cm: \n Inch = {dimensions_inch}\n cm = {dimensions_cm}")

# code for the same in pure python
dimensions = [1, 2, 3]
new_dimensions = [x*2.54 for x in dimensions]
print(new_dimensions)
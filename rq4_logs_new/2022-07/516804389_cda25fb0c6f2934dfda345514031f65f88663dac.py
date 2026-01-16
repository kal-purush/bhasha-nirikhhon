# plotly and dash crash course in NumPy

# import numpy
import numpy as np

# convert a list into an array

# create a list
my_list = [1,2,3,4]
my_list
type(my_list)

# use np.array to convert to a NumPy array for plotly and dash
arr = np.array(my_list)
arr
type(arr)

# use the arange function for numpy arrays (similar to range() function)
a = np.arange(0,10)
a
a = np.arange(0,10, 2) # add a step size of 2
a

# create a 5 x 5 array of 0's (floating point)
np.zeros((5,5))

# create a 10 x 10 array of 1's (floating point)
np.ones((10,10))

# create random numbers
np.random.randint(0,100) # will return a random number between supplied range
 
# create a 5 x 5 array of random numbers
np.random.randint(0,100, (5,5))

# create a linear spaced array
np.linspace(0, 10, 101) # returns 101 spaced numbers between 0 and 10

# set seed to the random number generator
np.random.seed(101)

# investigate values of an array
arr = np.random.randint(0,100,10)
arr.max() # max value
arr.min() # min value
arr.mean() # mean value
arr.argmax() # index value of the maximum value
arr.argmin() # index value of the minimum value

# reshape an array
arr.reshape(2,5)

# indexing [row, column]
mat = np.arange(0,100).reshape(10,10)

# to grab 52
mat[5, 2]

# to grab column 2
mat[:, 2]

# returns a boolean
mat > 50

# use this boolean to filter the data
mat[mat > 50]
import numpy as np
from numpy.linalg import eig
from scipy.linalg import solve
import matplotlib.pyplot as plt
from scipy.optimize import minimize


# 3.2 Numpy

# (1) Why is numpy faster than using for loops in Python for operations such as matrix multiplications?
# numpy in python processes data much faster than python loops.


# (2) What is the data type used in numpy which makes such operations feasible? Also name a few
# differences between this data type and its similar counterpart in Python
# NumPy's array class ndarray, also known by alias array.


# (3) Go through the documentation and create a numpy array with the elements [1,2,3,4]
numpy_array = np.array([1,2,3,4])
print(numpy_array)


# (4) Use np.ones, np.zeros to create an array of 1’s with dimension 3x4 and an array of 0’s with dimension 4x3.
numpy_ones = np.ones((3, 4))
print(numpy_ones)

numpy_zeros = np.zeros((3, 4))
print(numpy_zeros)


# (5) Create a 2x3 matrix A and a 3x4 matrix B and perform a matrix multiplication using numpy.
A = np.array([[1, 2, 3], [4, 5, 6]])
B = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
product = A @ B
print(product)

# (6) Find the eigenvalues and eigenvectors of the matrix given below?
value = np.array([[3, 1], [1, 2]])
x,y = eig(value)
print("Eigenvalue: ", x)
print("Eigenvector: ", y)


# 3.3 Matplotlib

a = np.linspace(0, 2 * np.pi, 100)
b = np.sin(a)
plt.plot(a, b)
plt.xlabel('x')
plt.ylabel('sin(x)')
plt.title('Sine Function')
plt.show()

fig, ax = plt.subplots()
ax.plot(np.sin(np.sqrt((x ** 2) + (y ** 2))))

# 3.4 SciPy

A = [[3, 1], [1, 2]]
b = [9, 8]
x = solve(A, b)
print("x = {x[0]}, y = {x[1]}")


def func(x):
    return x**2 + 2*x

x0 = 0
result = minimize(func, x0)
print(f"The minimum value: {result.fun}")
print(f"The value of x: {result.x[0]}")


x = np.linspace(0, 1, 1000)
f = np.sin(100 * np.pi * x) + 0.5 * np.sin(160 * np.pi * x)
fhat = fft(f)
plt.plot(fhat)
plt.show()


#3.5 Open CV

import conv


image = conv.imread('milner_image.png')
gray = conv.cvtColor(image, conv.COLOR_BGR2GRAY)
conv.imshow('gray', gray)
conv.waitKey(0)


edges = conv.Canny(gray, 100, 200)
conv.imshow('edges', edges)
conv.waitKey(0)


face_cascade = conv.CascadeClassifier('milner_haarcascade_frontalface_default.xml')
faces = face_cascade.detectMultiScale(gray, 1.1, 4)
for (x, y, w, h) in faces:
    conv.rectangle(image, (x, y), (x + w, y + h), (255, 0, 0), 2)
conv.imshow('faces', image)
conv.waitKey(0)

# Haar cascades detect faces by using Haar-like features, which are patterns of intensity differences (e.g., edges or lines) between regions in an image. 
# These features are quickly calculated using an integral image, and a cascade of classifiers, trained with AdaBoost, filters out non-face regions while 
# focusing computational effort on likely face areas. The cascade structure ensures efficient detection by evaluating simple features first and progressively 
# focusing on more complex patterns.
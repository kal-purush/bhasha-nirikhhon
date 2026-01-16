# 3.2

# (1) Why is numpy faster than using for loops in Python for operations such as matrix multiplications?
# numpy is faster than using for loops in Python for operations such as matrix multiplications because
# numpy is implemented in C and C++ which are faster than Python.

# (2) What is the data type used in numpy which makes such operations feasible? Also name a few
# differences between this data type and its similar counterpart in Python
# The data type used in numpy which makes such operations feasible is numpy.ndarray. The numpy.ndarray
# data type is similar to Python list but it is more efficient than Python list. The numpy.ndarray data
# type is homogeneous.
# (3) Go through the documentation and create a numpy array with the elements [1,2,3,4]
import numpy as np

arr = np.array([1, 2, 3, 4])
# (4) Use np.ones, np.zeros to create an array of 1’s with dimension 3x4 and an array of 0’s with
# dimension 4x3.
arr1 = np.ones((3, 4))
arr2 = np.zeros((4, 3))

# (5) Create a 2x3 matrix A and a 3x4 matrix B and perform a matrix multiplication using numpy.

A = np.array([[1, 2, 3], [4, 5, 6]])
B = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
C = np.matmul(A, B)
print(f'A*B = {C}')

# (6) Find the eigenvalues and eigenvectors of the matrix given below?
# [[3, 1], [1, 2]]
D = np.array([[3, 1], [1, 2]])
eigenvalues, eigenvectors = np.linalg.eig(D)
print(f'{eigenvalues=}')
print(f'{eigenvectors=}')

# 3.3
# Matplotlib
# Documentation: https://matplotlib.org/stable/tutorials/introductory/quick_start.html
# (1) Create a line plot of the sine function over the interval [0, 2π] using Matplotlib.
import matplotlib.pyplot as plt

x = np.linspace(0, 2 * np.pi, 100)
y = np.sin(x)
plt.plot(x, y)
# (2) Add labels to the axes in a Matplotlib plot.
plt.xlabel('x')
plt.ylabel('sin(x)')
# (3) Plot the 3d graph of the function given below using Matplotlib.
# 𝑧 = 𝑠𝑖𝑛(𝑥^2 + 𝑦^2)

from mpl_toolkits.mplot3d import Axes3D

x = np.linspace(-5, 5, 100)
y = np.linspace(-5, 5, 100)
x, y = np.meshgrid(x, y)
z = np.sin(x ** 2 + y ** 2)
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.plot_surface(x, y, z)
plt.show()

# 3.4. SciPy
# Documentation: https://docs.scipy.org/doc/scipy/tutorial/index.html#user-guide
# (1) Solve the linear system of equations given below using SciPy.
# 3𝑥 + 𝑦 = 9
# 𝑥 + 2 𝑦 = 8

from scipy.linalg import solve

A = np.array([[3, 1], [1, 2]])
b = np.array([9, 8])
x = solve(A, b)
print(f'{x=}')


# (2) Find the minimum of the function given below using SciPy's optimization module.
# 𝑦 = 𝑥2 + 2𝑥
from scipy.optimize import minimize


def func(x):
    return x ** 2 + 2 * x


x0 = 0
res = minimize(func, x0)
print(f'{res.x=}')

# (3) Perform the Fourier transformation of the function given below using SciPy. Plot the frequency
# response using matplotlib.
# 𝑓(𝑥) = 𝑠𝑖𝑛(100π𝑥) + 1/2 * 𝑠𝑖𝑛(160π𝑥)

from scipy.fft import fft

x = np.linspace(0, 1, 1000)
f = np.sin(100 * np.pi * x) + 0.5 * np.sin(160 * np.pi * x)
fhat = fft(f)
plt.plot(fhat)
plt.show()

# 3.5. OpenCV
# Documentation: https://docs.opencv.org/4.x/
# (1) Read an image and convert it to grayscale using OpenCV.
import cv2

image = cv2.imread('image.png')
gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
cv2.imshow('gray', gray)
cv2.waitKey(0)

# (2) Perform edge detection on the image using OpenCV.
edges = cv2.Canny(gray, 100, 200)
cv2.imshow('edges', edges)
cv2.waitKey(0)

# (3) Use a Haar cascade classifier to implement face detection using OpenCV on an image which
# contains faces.
face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')
faces = face_cascade.detectMultiScale(gray, 1.1, 4)
for (x, y, w, h) in faces:
    cv2.rectangle(image, (x, y), (x + w, y + h), (255, 0, 0), 2)
cv2.imshow('faces', image)
cv2.waitKey(0)
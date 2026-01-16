# 3.2 Numpy
# (1) Why is numpy faster than using for loops in Python for operations such as matrix multiplications?
# Numpy is faster than loops for matrix multiplication because it leverages vectorization. Vectorization 
# is when the program treats the data as a vector instead of a single value, which allows it to operate
# on many individual data points at one time by operating on the vector.

# (2) What is the data type used in numpy which makes such operations feasible? Also name a few
# differences between this data type and its similar counterpart in Python
# The data type used in numpy is numpy.array. The counterpart in Python is the list, and they differ
# in that the numpy arrays are designed to be multi-dimensional, while the lists are by design 1-D.
# If a user wants more than 1 dimension, they need to use a 1-D list of 1-D lists, which slows computing
# and makes them a little bit harder to work with. Numpy arrays also use significantly less memory in 
# general, and their operations are performed in C so they are much faster. Numpy arrays are homogeneous,
# which makes them less useful for more general coding projects than lists are but much faster than the
# heterogeneous lists

import numpy as np
import matplotlib.pyplot as plt
import scipy
import scipy.linalg
from scipy.fft import fft, fftfreq
import cv2 as cv
import argparse


array = np.array([1, 2, 3, 4])
ones = np.ones((3, 4))
zeroes = np.zeros((4, 3))
A = np.array([[1, 2, 3], [4, 5, 6]])
B = np.array([[7, 8 , 9, 10], [11, 12, 13, 14], [15, 16, 17, 18]])
product = np.matmul(A, B)
mat = np.array([[3, 1], [1, 2]])
eigVal, eigVec = np.linalg.eig(mat)
print("Eigenvalues are: ")
print(eigVal)
print("Eigenvectors are: ")
print(eigVec)


# 3.3 Matplotlib
x = np.linspace(0, 2*np.pi, 100)
y = np.sin(x)
fig, ax = plt.subplots()
plt.plot(x, y)
plt.xlabel('x')
plt.ylabel('sin(x)')
plt.title('Sine Wave')
plt.show()

y = np.linspace(0, 2*np.pi, 100)

fig, ax = plt.subplots(subplot_kw={"projection": "3d"})
x, y = np.meshgrid(x, y)
R = np.sqrt(x**2 + y**2)
z = np.sin(R)
surf = ax.plot_surface(x, y, z)
plt.show()

# 3.4 Scipy
a = np.array([[3, 1], [1, 2]])
b = np.array([[9], [8]])
x = scipy.linalg.solve(a, b)
print("Solution to system of linear equations: ")
print(x)

N = 600
T = 1.0/800.0
x = np.linspace(0.0, N*T, N, endpoint = False)
fx = np.sin(x*np.pi*100) + 0.5 * np.sin(x*np.pi*160)
yf = fft(fx)
xf = fftfreq(N, T)[:N//2]

plt.plot(xf, 2.0/N * np.abs(yf[0:N//2]))
plt.grid()
plt.show()

# OpenCV
img = "image.jpg"
src = cv.imread(img, cv.IMREAD_COLOR)
gray = cv.cvtColor(src, cv.COLOR_BGR2GRAY)
edges = cv.Canny(gray,100,200)
plt.subplot(121),plt.imshow(gray,cmap = 'gray')
plt.title('Original Image'), plt.xticks([]), plt.yticks([])
plt.subplot(122),plt.imshow(edges,cmap = 'gray')
plt.title('Edge Image'), plt.xticks([]), plt.yticks([])
plt.show()

img = cv.imread('people.jpg') 
gray_img = cv.cvtColor(img, cv.COLOR_BGR2GRAY) 
haar_cascade = cv.CascadeClassifier('Haarcascade_frontalface_default.xml') 
faces_rect = haar_cascade.detectMultiScale(gray_img, 1.1, 11) 
for (x, y, w, h) in faces_rect: 
    cv.rectangle(img, (x, y), (x+w, y+h), (0, 255, 0), 2) 
cv.imshow('Detected faces', img) 
cv.waitKey(0) 

# Explain how Haar cascades work to detect faces.
# The Haar cascade detects faces by convolving a small kernel across a grayscale image, using it to pull out
# instances of edges, lines, and four-rectangle features. The features are then filtered through Adaboost,
# which is an algorithm trained on manually classified data to extract the most useful features from the image.
# The goal is to get the most "definitely a face" or "definitely not a face" for each object detected to
# determine if the image is a face or not. The classifier then passes it through filter windows that use KNN to weed
# out things that aren't faces (for example, I originally had only 9 neighbors required to determine that an object
# was a face in the implementation above, and there were a couple of misdetections. I just manually ticked up the
# required number of neighbors to get rid of them, but a more robust algorithm would hopefully have that and other
# parameters exactly correct from training data). Anything immediately and obviously not a face is "cascaded" out,
# which allows the classifier to user the majority of its processing power to decide on objects that might go either
# way. The final decision is reached by identifying a potential face and then "zooming out" until it fails a filter
# or is classified as a face.
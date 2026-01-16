### 3.2 numPy ###

#1
# numPy is faster than using for loops because it applies the multiplication to the arrays at once, instead of each individual 
# element, like how the loops do. 

#2 
# The data type used in numpy that allows the operations to be feasible is numpy arrays. In python, the similar data type is 
# lists. From the google colab, the array in numpy has function in it that make the multiplication easier, while the 
# python lists do not. Python lists can store different data types in the same list, numpy arrays cannot.

#3 
import numpy as np
import scipy.optimize
print(np.array([1,2,3,4]))

#4
print(np.ones((3,4)))
print(np.zeros((4,3)))

#5
arr1 = np.array([[1,2,3],[3,4,5]])
arr2 = np.array([[7,8,9],[10,11,12],[13,14,15]])
print(np.dot(arr1,arr2))

#6
arr = np.array([[3,1],[1,2]])
eigenvalues, eigenvectors = np.linalg.eig(arr) 
print("Eigenvalues: ", eigenvalues)
print("\nEigenvectors: \n", eigenvectors)


### 3.3 Matplot ###
import matplotlib.pyplot as plt
from mpl_toolkits import mplot3d
x = np.linspace(0,2 * np.pi, 108)
y = np.sin(x)
plt.figure(figsize=(8,5))
plt.plot(x,y)
plt.xlabel("X-axis")
plt.ylabel("Y-axis")
plt.show()


from mpl_toolkits.mplot3d import Axes3D
x = np.linspace(-5, 5, 100)
y = np.linspace(-5, 5, 100)
x, y = np.meshgrid(x, y)
z = np.sin(np.sqrt(x**2 + y**2))
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.plot_surface(x, y, z)
plt.show()

### 3.4 SciPy ###

#1
import scipy
from scipy.linalg import solve

# coefficient matrix
A = np.array([[3, 1], [1, 2]])

# Right side of vector
b = np.array([9, 8])

# Solve the linear equation system
x = solve(A, b)

print("\nAnswers: \n", x)

#2 
def f(x):
    return x**2 + 2*x

# Initial guess
x0 = 0

# Minimize the function
result = scipy.optimize.minimize(f, x0)

print("\nX value is: \n", result.x)
print("Y value is: \n", result.fun)


#3
from scipy.fft import fft, fftfreq
def f(x):
    return np.sin(100 * np.pi * x) + 0.5 * np.sin(160 * np.pi * x)
x = np.linspace(-1, 1, 1000)
y = f(x)
Y = fft(y)
frequencies = fftfreq(len(x), x[1] - x[0])
plt.figure()
plt.plot(frequencies, np.abs(Y))
plt.show()

### 3.5 OpenCv ###
# 1 and 2
import numpy as np
import cv2 as cv
from matplotlib import pyplot as plt


img = cv.imread('bg3.png', cv.IMREAD_GRAYSCALE)
assert img is not None, "file could not be read, check with os.path.exists()"

edges = cv.Canny(img,100,200)

plt.figure(figsize=(12, 6))

plt.subplot(121),plt.imshow(img,cmap = 'gray')
plt.title('Original Image'), plt.xticks([]), plt.yticks([])
plt.subplot(122),plt.imshow(edges,cmap = 'gray')
plt.title('Edge Image'), plt.xticks([]), plt.yticks([])
plt.show()


# 3
def detect_faces(image_path):
    # Load the pre-trained Haar cascade classifier
    face_cascade = cv.CascadeClassifier(cv.data.haarcascades + 'haarcascade_frontalface_default.xml')

    # Read the image
    img = cv.imread(image_path)

    # Convert the image to grayscale
    gray = cv.cvtColor(img, cv.COLOR_BGR2GRAY)

    # Detect faces in the image
    faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5)

    # Draw rectangles around the detected faces
    for (x, y, w, h) in faces:
        cv.rectangle(img, (x, y), (x+w, y+h), (255, 0, 0), 2)

    # Display the image with the detected faces
    cv.imshow('Faces', img)
    cv.waitKey(0)
    cv.destroyAllWindows()

# Example usage
detect_faces('Face.png')

# Haar Cascade question
# Haar cascade detects faces by using a cascade function that is trained 
# to detect faces using negative and positive images. The model scans 
# through images in rectangle shaped windows and determines if it is a face
# or not based on its training. 
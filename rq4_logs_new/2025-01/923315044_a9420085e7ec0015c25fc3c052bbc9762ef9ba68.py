# 3.2

# 1
# NumPy applies operations to entire arrays in a single operation rather
# than looping through each element individually

# 2
# These operations are feasible due to arrays. It's similar counterpart in
# Python are lists. Some differences include the elements in NumPy arrays
# must be the same data type, whereas lists can store elements of different
# data types. Arrays are faster and lists are slower as well.

# 3
import numpy as np
array = np.array([1, 2, 3, 4])
print(array)

# 4
ones_array = np.ones((3, 4))
zeros_array = np.zeros((4, 3))
print(ones_array)
print(zeros_array)

# 5
A = np.array([[1, 2, 3], [4, 5, 6]])
B = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [1, 2, 3, 4]])
C = np.dot(A, B)
print(C)

# 6
D = np.array([[3, 1], [1, 2]])
print(D)
eigenvalues, eigenvectors = np.linalg.eig(D)
print("Eigenvalues:")
print(eigenvalues)
print("\nEigenvectors:")
print(eigenvectors)

# 3.3

# 1
import matplotlib.pyplot as plt
import numpy as np
x = np.linspace(0, 2 * np.pi, 100)
y = np.sin(x)
plt.figure(figsize=(8, 5))
plt.plot(x, y)

# 2
plt.xlabel("x")
plt.ylabel("sin(x)")
plt.show()

# 3
from mpl_toolkits.mplot3d import Axes3D
x = np.linspace(-5, 5, 100)
y = np.linspace(-5, 5, 100)
x, y = np.meshgrid(x, y)
z = np.sin(np.sqrt(x**2 + y**2))
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.plot_surface(x, y, z)
plt.show()

# 3.4

# 1
import numpy as np
from scipy.linalg import solve
A = np.array([[3, 1], [1, 2]])
b = np.array([9, 8])
x = solve(A, b)
print(x)

# 2
from scipy.optimize import minimize
def func(x): 
    return x**2 + 2*x
result = minimize(func, x0=0)
print("x =", result.x)
print("y =", result.fun)

# 3
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

# 3.5

# 1 and 2
import numpy as np
import cv2 as cv
from matplotlib import pyplot as plt

img = cv.imread('download.jpg', cv.IMREAD_GRAYSCALE)

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
detect_faces('IMG_5506.jpeg')

# HW
# Haar cascade detects faces by using a cascade function that is trained
# to detect faces using negative and positive images. The model scans
# through images in rectangle shaped windows and determines if it is a face
# or not based on its training.
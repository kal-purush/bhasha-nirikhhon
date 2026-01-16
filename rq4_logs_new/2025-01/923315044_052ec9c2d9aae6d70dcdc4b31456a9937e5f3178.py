print("Hello World")
### NUMPY
#Problem 1
print(" NumPy is faster because it uses vectorized operations implemented in low-level languages like C and Fortran, avoiding the overhead of Python loops. Additionally, it utilizes optimized libraries like BLAS and LAPACK and stores data in contiguous memory blocks for efficient access.")


#Problem 2
print()
print("NumPy uses the ndarray data type, which is a homogeneous, n-dimensional array optimized for numerical operations. Unlike Python lists, ndarray is more memory-efficient, faster, and supports vectorized operations directly without loops.")


#Problem 3
import numpy as np

array = np.array([1, 2, 3, 4])
print(array)

#Problem 4

ones_array = np.ones((3, 4))
zeros_array = np.zeros((4, 3))
print(ones_array)
print(zeros_array)


# Prolem 5

import numpy as np

# Create a 2x3 matrix A
A = np.array([[1, 2, 3], [4, 5, 6]])

# Create a 3x4 matrix B
B = np.array([[7, 8, 9, 10], [11, 12, 13, 14], [15, 16, 17, 18]])

# Perform matrix multiplication
result = np.dot(A, B)

print("Matrix A:")
print(A)

print("\nMatrix B:")
print(B)

print("\nResult of A x B:")
print(result)


# Problem 6

import numpy as np

# Define the matrix
matrix = np.array([[3, 1], [1, 2]])

# Compute eigenvalues and eigenvectors
eigenvalues, eigenvectors = np.linalg.eig(matrix)

# Print results
print("Eigenvalues:")
print(eigenvalues)

print("\nEigenvectors:")
print(eigenvectors)


### MatplotLib

#Problem 1
import matplotlib.pyplot as plt
import numpy as np

# Generate x values from 0 to 2π
x = np.linspace(0, 2 * np.pi, 500)
y = np.sin(x)

# Create the line plot
plt.plot(x, y)
plt.title("Sine Function")
plt.xlabel("x")
plt.ylabel("sin(x)")
plt.grid()
plt.show()


#Problem 2
# Example with a cosine function
x = np.linspace(0, 2 * np.pi, 500)
y = np.cos(x)

plt.plot(x, y, label="cos(x)")
plt.xlabel("x-axis (radians)")
plt.ylabel("y-axis (cosine)")
plt.title("Cosine Function with Axis Labels")
plt.legend()
plt.grid()
plt.show()


#Problem 3
from mpl_toolkits.mplot3d import Axes3D

# Generate x and y values
x = np.linspace(0, 2*np.pi, 100)
y = np.linspace(0, 2*np.pi, 100)
x, y = np.meshgrid(x, y)
z = np.sin(np.sqrt(x**2 + y**2))

# Create a 3D plot
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.plot_surface(x, y, z, cmap='viridis')

# Add labels
ax.set_xlabel('X-axis')
ax.set_ylabel('Y-axis')
ax.set_zlabel('Z-axis')

plt.title("3D Plot of z = sin(√(x² + y²))")
plt.show()


###SciPy

#Problem 1
from scipy.linalg import solve
import numpy as np

# Define the coefficients matrix A and vector b
A = np.array([[3, 1], [1, 2]])
b = np.array([9, 8])

# Solve the system of equations
solution = solve(A, b)
print("Solution (x, y):", solution)


#Problem 2
from scipy.optimize import minimize

# Define the function
def func(x):
    return x**2 + x

# Minimize the function starting from an initial guess (e.g., x = 0)
result = minimize(func, x0=0)

# Print the result
print("Minimum value of y:", result.fun)
print("Value of x at minimum:", result.x)

#Problem 3 
from scipy.fft import fft, fftfreq
import matplotlib.pyplot as plt

# Define the signal
fs = 1000  # Sampling frequency
t = np.linspace(0, 1, fs, endpoint=False)  # Time interval
f_x = np.sin(100 * np.pi * t) + 0.5 * np.sin(160 * np.pi * t)

# Perform the Fourier transform
fft_result = fft(f_x)
freqs = fftfreq(len(t), 1 / fs)

# Plot the frequency response
plt.figure(figsize=(8, 6))
plt.plot(freqs[:fs//2], np.abs(fft_result)[:fs//2])  # Only plot positive frequencies
plt.title("Frequency Response")
plt.xlabel("Frequency (Hz)")
plt.ylabel("Amplitude")
plt.grid()
plt.show()

###CSV 
#Problem 1
import cv2
import os

# Read the image
image = cv2.imread(".\lab_env\image.jpg")  # Replace "image.jpg" with the path to your image
if image is None:
    print("Error: Unable to load image. Check the file path.")
    print("Current Working Directory:", os.getcwd())
    exit()
# Convert to grayscale
gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# Display the original and grayscale images
cv2.imshow("Original Image", image)
cv2.imshow("Grayscale Image", gray_image)

# Wait and close windows
cv2.waitKey(0)
cv2.destroyAllWindows()


#Problem 2
 # Apply Canny edge detection
edges = cv2.Canny(gray_image, threshold1=100, threshold2=200)

# Display the edges
cv2.imshow("Edge Detection", edges)

# Wait and close windows
cv2.waitKey(0)
cv2.destroyAllWindows()


#Problem 3
import cv2

# Load the Haar cascade classifier
face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")

# Read the input image
# image = cv2.imread("faces.jpg")  # Replace "faces.jpg" with the path to your image

# Convert the image to grayscale
gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# Detect faces in the image
faces = face_cascade.detectMultiScale(
    gray_image,
    scaleFactor=1.1,  # How much the image size is reduced at each image scale
    minNeighbors=5,   # How many neighbors each candidate rectangle should have to retain it
    minSize=(30, 30)  # Minimum size of detected face
)

# Draw rectangles around detected faces
for (x, y, w, h) in faces:
    cv2.rectangle(image, (x, y), (x+w, y+h), (255, 0, 0), 2)  # Blue rectangle

# Display the output image
cv2.imshow("Face Detection", image)

# Wait for a key press and close the windows
cv2.waitKey(0)
cv2.destroyAllWindows()


### Homework

print("Haar cascades detect faces using a combination of **Haar-like features** and a **cascade of classifiers**. Haar-like features represent contrasts in pixel intensities, such as edges, lines, and rectangles, which are common patterns in facial structures (e.g., the contrast between the eyes and the forehead). These features are computed efficiently using the **integral image**, which allows for fast summation of pixel intensities over rectangular regions. A Haar cascade works by passing an image through multiple stages of classifiers, each trained to detect a specific set of features. If a region of the image passes all stages, it is classified as containing a face, while non-face regions are quickly discarded. This cascading structure ensures that the algorithm is computationally efficient and suitable for real-time applications.\nThe training of Haar cascades involves a large dataset of positive (e.g., faces) and negative samples (e.g., non-faces), which helps identify the most discriminative features for object detection. Haar cascades also scan images at multiple scales to detect faces of varying sizes. While they are lightweight and effective for frontal face detection, they can struggle with complex backgrounds, lighting variations, or non-frontal faces. Newer methods like HOG (Histogram of Oriented Gradients) and deep learning-based approaches (e.g., CNNs) often outperform Haar cascades in terms of accuracy and robustness, but Haar cascades remain a useful choice for lightweight and real-time applications.")
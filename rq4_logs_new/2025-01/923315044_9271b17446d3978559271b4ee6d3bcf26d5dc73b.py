import numpy as np
import matplotlib.pyplot as plt
from scipy import linalg
import scipy
from scipy.fft import fft, fftfreq
import cv2
#3.2 Numpy

#1. Numpy uses vectorization rather than looping/iterating through a list
#Numpy operations are implemented in C, which are fast and optimized operations
#Numpy is also densley packed in memory

#2. Primary data type is a multidimensional arrays (ndarray).
#The primary difference between ndarray and python's array is that python's
#object only handles 1D arrays, and yields less capabilities compared to ndarray
#Not only this, ndarray is a set size of type tuple, whereas array.array is not

#3.
array = np.array([1,2,3,4])
onesarray = np.ones((3,4))
zerosarray = np.zeros((4,3))
A = np.array([[1,3,4],[4,5,6]])
B = np.array([[9,8,7,6],[5,4,3,1],[2,2,2,2]])
#print defined arrays!
print(array, "\n", onesarray, "\n", zerosarray, "\n")
print(A,"\n")
print(B,"\n")
product = A@B #Matrix multiplication
print(product,"\n")
eigenmatrix = np.array([[3,1], [1,2]])
print(eigenmatrix,"\n")
print(np.linalg.eig(eigenmatrix),"\n") #Print eigenvalues and eigenvectors

#3.3 Matplotlib
#Make the sine plot
X = np.linspace(0,2*np.pi,32)
Y = np.sin(X)
plt.plot(X,Y)
plt.ylabel("y-axis")
plt.xlabel("x-axis")
plt.title("Sample title!")
plt.show()
#Now 3D space!
x = np.linspace(-10,10,500)
y = np.linspace(-10,10,500)
z = np.sin(np.sqrt(x**2+y**2))

fig = plt.figure()
ax = plt.axes(projection = '3d')
ax.plot3D(x,y,z)
ax.set_title('3D plot!!!')
plt.show()

#3.4 SciPy
a = np.array([[3,1],[1,2]])
b = np.array([[9],[8]])
sol = linalg.solve(a,b) #Solve system of eqns
print("solution is","\n",sol)
#Find min
def function (x):
    return (x**2 + 2*x)
print("Minimum is ",scipy.optimize.minimize(function,1).x)
#Fourier transform, reusing x linspace from previous problems
N = 600 #Sample points
T = 1/800
x = np.linspace(0,N*T,N)
y = np.sin(100*np.pi*x) + 0.5*(np.sin(160*np.pi*x))
yf = fft(y)
xf = fftfreq(N,T)[:N//2]
plt.plot(xf,2.0/N*np.abs(yf[0:N//2]))
plt.xlabel("Frequency")
plt.ylabel("Amplitude")
plt.title("Frequency Response")
plt.show()

#3.5 OpenCV
#Greyscale image
img = cv2.imread("GOAT.jpg", 0)
cv2.imshow('GOAT',img)
cv2.waitKey(0)
cv2.destroyAllWindows()
#edge detection
edges = cv2.Canny(img,100,200)
plt.subplot(121),plt.imshow(img,cmap = 'gray')
plt.title('Original GOAT')
plt.subplot(122),plt.imshow(edges,cmap = 'gray')
plt.title('GOAT w/ edge detection')
plt.show()
#Haar cascade 
#load in colored image
img = cv2.imread("GOAT.jpg", 1)
#load object
face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
#detect face
faces = face_cascade.detectMultiScale(img, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))
#Draw rectangle around faces
for (x,y,w,h) in faces:
    cv2.rectangle(img, (x, y), (x + w, y + h), (255, 0, 0), 2)
#show output image
cv2.imshow("Detected GOAT", img)
cv2.waitKey(0)
cv2.destroyAllWindows
#In this case, we are using a pretrained .xml file to look for similarities between the
#user input image and the trained data. On a lower level, it looks for pixel intensities at specific
#areas, edge features, line features, and more.
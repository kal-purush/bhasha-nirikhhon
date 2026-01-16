import cv2 as cv
import numpy as np

img = cv.imread("girl.jpg")
kernel = np.ones((5,5),np.uint8)

imgGray = cv.cvtColor(img,cv.COLOR_BGR2GRAY)
imgBlur = cv.GaussianBlur(imgGray,(7,7),0)
imgCanny = cv.Canny(img,150,200)
imgDialation = cv.dilate(imgCanny,kernel,iterations=5)
imgEroded = cv.erode(imgDialation,kernel,iterations=1)

cv.imshow("Gray image",imgGray)
cv.imshow("Blur image",imgBlur)
cv.imshow("Canny image",imgCanny)
cv.imshow("Dialation image",imgDialation)
cv.imshow("Eroded image",imgEroded)
cv.waitKey(0)

#resizing and cropping
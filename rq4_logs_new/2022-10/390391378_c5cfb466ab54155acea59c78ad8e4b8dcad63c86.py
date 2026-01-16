import cv2 as cv
import numpy as np
from google.colab.patches import cv2_imshow # This is needed only for google colab

"""OpenCV reads images in BGR format not RGB format"""

blank = np.zeros((500,500,3), dtype='uint8')
cv2_imshow(blank)
cv.waitKey(0)

# 1. Paint the image a certain colour in a certain position
blank[200:300, 300:400] = 255,0,0
#cv.imshow('Blue', blank) #If your running in your local computer you should use this
cv2_imshow(blank) #cv2_imshow is only needed to be run in google colab

cv.waitKey(0)

# 1. Paint the image a certain colour totally
blank[:] = 255,0,0
#cv.imshow('Blue', blank)
cv2_imshow(blank)
cv.waitKey(0)

# 2. Draw a Rectangle
cv.rectangle(blank, (0,0), (250, 500), (0,255,0), thickness=5)
#cv.imshow('Rectangle', blank)
cv2_imshow(blank)
cv.waitKey(0)

# 2. Draw a Rectangle
cv.rectangle(blank, (0,0), (blank.shape[1]//2, blank.shape[0]//2), (0,255,0), thickness=-1)
#cv.imshow('Rectangle', blank)
cv2_imshow(blank)
cv.waitKey(0)

# 3. Draw A circle
cv.circle(blank, (blank.shape[1]//2, blank.shape[0]//2), 40, (0,0,255), thickness=-1)
#cv.imshow('Circle', blank)
cv2_imshow(blank)
cv.waitKey(0)

# 4. Draw a line
cv.line(blank, (0,250), (300,400), (255,255,255), thickness=3)
#cv.imshow('Line', blank)
cv2_imshow(blank)
cv.waitKey(0)

# 5. Write text
cv.putText(blank, 'Hello, my name is Modassir!!!', (0,225), cv.FONT_HERSHEY_TRIPLEX, 2.0, (255,255,0), 2)
#cv.imshow('Text', blank)
cv2_imshow(blank)
cv.waitKey(0)
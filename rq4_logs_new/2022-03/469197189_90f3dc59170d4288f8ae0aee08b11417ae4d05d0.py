import time
import cv2
#from networktables import NetworkTable, NetworkTables as NT, NetworkTablesInstance as NTI
import numpy as np

window = 'colorfinder'

sink = cv2.VideoCapture(1)
input_img = 0

# [hue, saturation, value]
blueMin = np.asarray([0, 180, 0])
blueMax = np.asarray([30, 200, 255])

redMin = np.asarray([110, 180, 0])
redMax = np.asarray([120, 200, 255])

isred = True

time.sleep(6)
while True:
   
   times, input_img = sink.read()
   output_img = np.copy(input_img)
  

   #Takes input and converts its to a binary image (to do: make not bad)
   HSV_img = cv2.cvtColor(input_img, cv2.COLOR_RGB2HSV)
   if isred == True:
      binary_img = cv2.inRange(HSV_img, redMin, redMax)
   else:
      binary_img = cv2.inRange(HSV_img, blueMin, blueMax)

   kernel = np.ones((3, 3), np.uint8)
   binary_img = cv2.morphologyEx(binary_img, cv2.MORPH_OPEN, kernel)


   cv2.waitKey(10)
   cv2.imshow(window, binary_img)
import cv2
import numpy as np
import time
import serial

camera = cv2.VideoCapture(0) #open camera
i = 0
j=0 
redColor=False
brownColor= False
lenContRed=0
lenContBrown=0

def contoursFilter():

  for cnt in contours:  ##-----Iterate all contours-------------##
     #cnt = contours[i]
     M = cv2.moments(cnt)
     print M
     
     #Find centroid
     m00 = M['m00']
     centroid_x, centroid_y = None, None
     if m00 != 0:
       centroid_x = int(M['m10']/m00)
       centroid_y = int(M['m01']/m00)

     # Assume no centroid
     ctr = (-1,-1)

     # Use centroid if it exists
     if centroid_x != None and centroid_y != None:

        print('coordinates contour '+str(j)+':')
        print(centroid_x,centroid_y)
        ctr = (centroid_x, centroid_y)

     else:
       print('no mass center in contour'+ str(i))

     j=j+1

  return contours


def redFilter(hsv):
  lower_range = np.array([0, 50, 50], dtype=np.uint8) #red color
  upper_range = np.array([10, 255, 255], dtype=np.uint8)

  mask = cv2.inRange(hsv, lower_range, upper_range)

  #Remove noise of the selected mask
  kernel = np.ones((5,5),np.uint8)
  erosion = cv2.erode(mask, kernel, iterations=1)
  erosion2 = cv2.erode(erosion, kernel, iterations=1)
  erosion3 = cv2.erode(erosion2, kernel, iterations=1)
  dilation = cv2.dilate(erosion3,kernel, iterations =1)
  dilation2 = cv2.dilate(dilation,kernel, iterations =1) 
  dilation3 = cv2.dilate(dilation2,kernel, iterations =1)

  ##-----Threshold Filter-------------##
  ret,thresh = cv2.threshold(img,127,255,0)
  ##-----Find contours-------------##
  im,contours,hierarchy = cv2.findContours(thresh, 1, 2)

  contRed= contoursFilter()
  
  lenContRed= len(contRed)

  if (lenContRed > 1):
    redColor=True

  else 
    redColor=False


def brownFilter(hsv):
  lower_blue = np.array([20,50,50])
  upper_blue = np.array([40,255,255])

  mask = cv2.inRange(hsv, lower_range, upper_range)

  #Remove noise of the selected mask
  kernel = np.ones((5,5),np.uint8)
  erosion = cv2.erode(mask, kernel, iterations=1)
  erosion2 = cv2.erode(erosion, kernel, iterations=1)
  erosion3 = cv2.erode(erosion2, kernel, iterations=1)
  dilation = cv2.dilate(erosion3,kernel, iterations =1)
  dilation2 = cv2.dilate(dilation,kernel, iterations =1) 
  dilation3 = cv2.dilate(dilation2,kernel, iterations =1)

  ##-----Threshold Filter-------------##
  ret,thresh = cv2.threshold(img,127,255,0)
  ##-----Find contours-------------##
  im,contours,hierarchy = cv2.findContours(thresh, 1, 2)

  contBrown= contoursFilter()
  
  lenContBrown= len(contBrown)

  if (lenContRed > 1):
    brownColor=True

  else 
    brownColor=False


while i < 10:               #take 10 pictures to get the good one
    print('take frame'+str(i))
    return_value, image = camera.read()
    cv2.imwrite('nao'+str(i)+'.jpg', image)
    i=i+1
del(camera)

img=cv2.imread('nao9.jpg')  #take the last image (the good one)
cv2.imshow('nao9',img)

img = cv2.imread('nao9.jpg', 1) #take the last image (the good one)
hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

if (redFilter == False):
  redFilter(hsv)

elif(brownFilter == False ):
  brownFilter(hsv)

else:
  print("No color detected")


if (redColor == True):
  print("Red detected")

elif (brownColor == True):
  print("Brown detected")

else:
  print("no color detected")

while(1):
   k = cv2.waitKey(0)
   if(k == 27):
      break
      
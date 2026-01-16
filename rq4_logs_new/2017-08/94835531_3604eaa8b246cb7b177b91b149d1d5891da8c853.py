import cv2
import numpy as np

camera = cv2.VideoCapture(0) #open camera
i = 0

redColor=False
brownColor= False
lenContRed=0
lenContBrown=0
lenContWhite=0

def contoursFilter():

  ##-----Read Mask--------------------##
  img = cv2.imread('dilation3.png',0)
  ##-----Threshold Filter-------------##
  ret,thresh = cv2.threshold(img,127,255,0)
  ##-----Find contours-------------##
  im,contours,hierarchy = cv2.findContours(thresh, 1, 2)

  ##-----Iterate all contours-------------##
  i=0
  for cnt in contours:
    M = cv2.moments(cnt)
    #print M
     
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
      ctr = (centroid_x, centroid_y)
      #Draw white circle in at centroid in image
      cv2.circle(img, ctr, 30, (255,255,255),5)

    #else:
      #print('no mass center in contour'+ str(i))

    i=i+1
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

  #cv2.imshow('dilation3',dilation3)
  cv2.imwrite('dilation3.png', dilation3)
  
  contRed= contoursFilter()
    
  lenContRed= len(contRed)

  #print("Length Contours: "+str(lenContRed))

  if(lenContRed >= 1):
    return True

  else:
    return False


def brownFilter(hsv):
  lower_range = np.array([20, 50, 50], dtype=np.uint8) 
  upper_range = np.array([40, 255, 255], dtype=np.uint8)

  mask = cv2.inRange(hsv, lower_range, upper_range)

  #Remove noise of the selected mask
  kernel = np.ones((5,5),np.uint8)
  erosion = cv2.erode(mask, kernel, iterations=1)
  erosion2 = cv2.erode(erosion, kernel, iterations=1)
  erosion3 = cv2.erode(erosion2, kernel, iterations=1)
  dilation = cv2.dilate(erosion3,kernel, iterations =1)
  dilation2 = cv2.dilate(dilation,kernel, iterations =1) 
  dilation3 = cv2.dilate(dilation2,kernel, iterations =1)

  #cv2.imshow('dilation3',dilation3)
  cv2.imwrite('dilation3.png', dilation3)

  contBrown= contoursFilter()
  
  lenContBrown= len(contBrown)

  if(lenContBrown >= 1):
    return True

  else:
    return False

def whiteFilter(hsv):
  lower_range = np.array([0, 0, 140], dtype=np.uint8) #red color
  upper_range = np.array([0, 255, 255], dtype=np.uint8)

  mask = cv2.inRange(hsv, lower_range, upper_range)

  #Remove noise of the selected mask
  kernel = np.ones((5,5),np.uint8)
  erosion = cv2.erode(mask, kernel, iterations=1)
  erosion2 = cv2.erode(erosion, kernel, iterations=1)
  erosion3 = cv2.erode(erosion2, kernel, iterations=1)
  dilation = cv2.dilate(erosion3,kernel, iterations =1)
  dilation2 = cv2.dilate(dilation,kernel, iterations =1) 
  dilation3 = cv2.dilate(dilation2,kernel, iterations =1)

  #cv2.imshow('dilation3',dilation3)
  cv2.imwrite('dilation3.png', dilation3)
  
  contWhite= contoursFilter()
    
  lenContWhite= len(contWhite)

  #print("Length Contours: "+str(lenContRed))

  if(lenContWhite >= 1):
    return True

  else:
    return False


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

whiteColor =whiteFilter(hsv)
    
if (whiteColor == True):
  print("white detected")

else:
  print("no color detected")

while(1):
   k = cv2.waitKey(0)
   if(k == 27):
      break
      
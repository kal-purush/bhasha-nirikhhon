import cv2
import numpy as np

camera = cv2.VideoCapture(0) #open camera
i = 0

redColor=False
brownColor= False
whiteColor=False
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

  dilation3Red = dilation3
  cv2.imshow('dilation3Red',dilation3Red)
  cv2.imwrite('dilation3.png', dilation3)
  
  contRed= contoursFilter()
  
  global lenContRed

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

  dilation3Brown = dilation3
  cv2.imshow('dilation3Brown',dilation3Brown)
  cv2.imwrite('dilation3.png', dilation3)

  contBrown= contoursFilter()

  global lenContBrown
  
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

  dilation3White = dilation3
  cv2.imshow('dilation3White',dilation3White)
  cv2.imwrite('dilation3.png', dilation3)
  
  contWhite= contoursFilter()
  
  global lenContWhite
  lenContWhite= len(contWhite)

  #print("Length Contours: "+str(lenContRed))

  if(lenContWhite >= 1):
    return True

  else:
    return False


while i < 10:               #take 10 pictures to get the good one
    print('take frame'+str(i))
    return_value, image = camera.read()
    cv2.imwrite('nao_'+str(i)+'.jpg', image)
    i=i+1
del(camera)

img=cv2.imread('nao_9.jpg')  #take the last image (the good one)
cv2.imshow('nao9',img)
hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

redColor = redFilter(hsv) #return: true/false
brownColor =brownFilter(hsv)
whiteColor =whiteFilter(hsv)



if lenContWhite != 0 or lenContBrown != 0 or lenContRed != 0:

  l=[lenContWhite, lenContRed, lenContBrown]
  l.sort()
  print("l array:")
  print(l);

  colorDetected= l[2] #poner el nombre del color de la longitud mas larga
  print("colorDetected"+str(colorDetected))

  if colorDetected == lenContRed:
    print("Red detected") 

  elif colorDetected == lenContWhite:
    print("White detected")

  elif colorDetected == lenContBrown:
    print("Brown detected")

else:
  print("no color detected")


while(1):
   k = cv2.waitKey(0)
   if(k == 27):
      break
      
from cv2 import cv2
import numpy as np
file = r'C:\Users\Juliana\Downloads\radiografia_crop.jpg'
img = cv2.imread(file,0)
equ = cv2.equalizeHist(img)
res = np.hstack((img,equ)) #stacking images side-by-side
cv2.imshow('Imagen Resultado', res)
cv2.waitKey(0)
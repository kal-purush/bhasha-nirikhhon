import numpy as np
import cv2 as cv
import face_recognition
import os

path ="/home/kratos/Documents/opencv/photos"
listImg=os.listdir(path)

list=[]
namelistImg=[]
print(listImg)
for index in listImg:
    image = cv.imread(f'{path}/{index}')
    list.append(image)
    namelistImg.append(os.path.splitext(index)[0] )
print(namelistImg)
def encodeinImage(list):
    encoded=[]
    for img in list:
        img = cv.cvtColor(img,cv.COLOR_BGR2RGB,None,None)
        encode = face_recognition.face_encodings(img)[0]
        encoded.append(encode)
    return encoded
resultFinal = encodeinImage(list)
print(len(resultFinal))
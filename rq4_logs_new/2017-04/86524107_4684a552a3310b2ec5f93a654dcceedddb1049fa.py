import numpy as np
import cv2
import os

face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')
imgList = os.listdir("images")


def find_img(img, name):
    print ("./images/" + img)
    img = cv2.imread("./images/" + img,1)
    print img.shape
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    faces = face_cascade.detectMultiScale(gray, 1.3, 5)
    for (x, y, w, h) in faces:
        img = cv2.rectangle(img, (x, y), (x + w, y + h), (255, 0, 0), 2)
    cv2.imwrite("./output/"+name, img)


for img in imgList:
    print img
    find_img(img, img)
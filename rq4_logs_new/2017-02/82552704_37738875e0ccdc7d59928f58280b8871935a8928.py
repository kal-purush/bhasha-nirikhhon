#!/usr/bin/python
# -*- coding: utf-8 -*-
# Sunny Islam, 2017

# python can't refer to default-site packages folder. It's possible to edit ~/.bashrc to have a default workaround.
#
# here is workaround from stackoverflow, this is better if you use pyCharm:
# Try to add the following line in ~/.bashrc
# export PYTHONPATH=/usr/local/lib/python2.7/site-packages:$PYTHONPATH
#
# this is what i do:

import sys

sys.path.append('/usr/local/lib/python2.7/site-packages')

# import openCV

import cv2

# load our Haar Cascade
# this Haar Cascade is made by Intel
# you can get yours and several others from https://github.com/opencv/opencv/tree/master/data/haarcascades

face_cascade = \
    cv2.CascadeClassifier('haarcascade_frontalface_default.xml')

# this loads in our webcamera
# if you want to employ a video clip instead, you can use this:
# cap = cv2.VideoCapture('myVideoFile.mp4')

cap = cv2.VideoCapture(0)

while True:
    (ret, img) = cap.read()
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(gray, 1.3, 5)
    font = cv2.FONT_HERSHEY_DUPLEX
    x = len(faces)
    cv2.putText(
        img,
        'there are ' + str(x) + ' persons in this frame',
        (15, 15),
        font,
        0.5,
        (0xff, 0xff, 0xff),
        2,
        cv2.LINE_AA,
    )
    cv2.imshow('img', img)

    # this allows you to pause the videostream with any key

    k = cv2.waitKey(30) & 0xff

    # pressing the esc(ape)-key will terminate the script

    if k == 27:
        break

# discontinue videostream and close all windows

cap.release()
cv2.destroyAllWindows()

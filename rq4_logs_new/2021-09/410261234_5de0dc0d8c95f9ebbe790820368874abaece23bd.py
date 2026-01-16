# -*- coding: utf-8 -*-
"""
Created on Mon Aug 02 20:50:32 2021

@author: Shyam jee
"""


# FLIPPING THE IMAGE

from PIL import Image

# OPENING THE IMAGE

imgg=Image.open('C:\\Users\\sachin singh\\mirrorimage.jpeg')

# TRANSPOSING

transposed_img=imgg.transpose(Image.FLIP_LEFT_RIGHT)

# SAVE IT TO A FILE IN HUMAN UNDERSTANDABLE FORMAT

transposed_img.save('correctimage.jpeg')

print('Done flipping')


# IMAGE ENHANCEMENT CLAHE

import cv2

# READ THE IMAGE

img=cv2.imread('C:\\Users\\sachin singh\\Downloads\\bulletimage.jpg')


#PREPERATION FOR CLAHE

clahe=cv2.createCLAHE()


# CONVERT TO GRAY SCALE IMAGE


gray_img=cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)

#APPLY ENHANCEMENT

enh_img=clahe.apply(gray_img)

cv2.imwrite('enhancedimage.jpeg',enh_img)

print('Done enhancing')

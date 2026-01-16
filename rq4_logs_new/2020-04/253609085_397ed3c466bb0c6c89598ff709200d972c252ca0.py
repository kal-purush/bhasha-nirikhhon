from __future__ import print_function
from __future__ import division
import cv2 as cv
import numpy as np
import argparse


def plotBrightnessHistogram(src):
    src = cv.cvtColor(src, cv.COLOR_BGR2HSV)
    hsv_channels = cv.split(src)
    histSize = 255
    histRange = (0, 256) # the upper boundary is exclusive
    hist_h, hist_w , _ = src.shape

    #hist_w = 512
    #hist_h = 400
    bin_w = int(round( hist_w/histSize ))
    accumulate = False


    v_hist =   cv.calcHist(hsv_channels, [2], None, [histSize], histRange, accumulate=accumulate)
    cv.normalize(v_hist, v_hist, alpha=0, beta=hist_h, norm_type=cv.NORM_MINMAX)
    histImage = np.zeros((hist_h, hist_w, 3), dtype=np.uint8)

    for i in range(1, histSize):
        cv.line(histImage, ( bin_w*(i-1), hist_h - round(int((v_hist[i-1])) )),
        ( bin_w*(i), hist_h - round(int((v_hist[i]))) ),
        ( 255, 0, 0), thickness=2)


    return histImage, v_hist

def histogramSplitter(histogram):
    #Split into segments:
    # 0 - 51, 51 - 102, 102 - 153, 153-204, 204-255
    dark1, dark2, middle, light1, light2 = [], [], [], [], []
    range1 = range(0,51)
    range2 = range(51, 102)
    range3 = range(102,153)
    range4 = range(153, 204)
    range5 = range(204, 255)


    for i in histogram:
        brightness = round(i[0])
        
        if brightness in range1:
            dark1.append(brightness)
        if brightness in range2:
            dark2.append(brightness)
        if brightness in range3:
            middle.append(brightness)
        if brightness in range4:
            light1.append(brightness)
        if brightness in range5:
            light2.append(brightness)
    

    return dark1, dark2, middle, light1, light2

## [Load image]
bg_1 = '../data/bg/ABODA/video7/frm_00681.jpg'
bg_2 = '../data/bg/ABODA/video7/frm_04004.jpg'
bg1 = cv.imread(bg_1)
bg2 = cv.imread(bg_2)

histImage1, v_hist1 = plotBrightnessHistogram(bg1)
histImage2, v_hist2 = plotBrightnessHistogram(bg2)

hist1_dark1, hist1_dark2, hist1_middle, hist1_light1, hist1_light2 = histogramSplitter(v_hist1)
hist2_dark1, hist2_dark2, hist2_middle, hist2_light1, hist2_light2 = histogramSplitter(v_hist2)



gray1 = cv.cvtColor(bg1, cv.COLOR_BGR2GRAY)
blurred1 = cv.GaussianBlur(gray1, (11, 11), 0)
thresh1 = cv.threshold(blurred1, 100, 255, cv.THRESH_BINARY)[1]
thresh1 = cv.erode(thresh1, None, iterations=2)
thresh1 = cv.dilate(thresh1, None, iterations=4)
h,w = thresh1.shape

for i in range(0,h):
    for j in range(0,w):
        if (thresh1[i][j] == 255):
            bg1[i][j] = bg2[i][j]

cv.imwrite('../data/Lighting/img1.jpg', bg1)
cv.imwrite('../data/Lighting/img2.jpg', bg2)

thresh1 = cv.cvtColor(thresh1, cv.COLOR_GRAY2BGR)
histograms = np.hstack((histImage1, bg1, histImage2, bg2, thresh1))




while True:
   cv.imshow('original image', histograms) 
   #cv.imshow('calcHist Demo', histImage)
   k = cv.waitKey(1)
   if k == ord('q'):
       break
import numpy as np
import cv2 as cv
import sys
import glob
from operator import attrgetter
import math
import argparse
import random as rng


src = cv.imread('../sample-images/qr_test2.png', 1)


cv.imshow('Window title', src)
cv.waitKey(0)
cv.destroyAllWindows()


rng.seed(12345)
def thresh_callback(val):
    threshold = val

    # Detect edges using Canny
    canny_output = cv.Canny(src_gray, threshold, threshold * 2)

    # Find contours
    _, contours, hier = cv.findContours(canny_output, cv.RETR_TREE, cv.CHAIN_APPROX_SIMPLE)
    # contours.sort(key=cv.contourArea, reverse=True)

    print('Number of contours: {}'.format(len(contours)))
    print(contours[0])
    # contours = contours[:10]

    # Find the convex hull object for each contour
    hull_list = []
    for i in range(len(contours)):
        hull = cv.convexHull(contours[i])
        hull_list.append(hull)

    simplified_list = []
    for i in range(len(contours)):
        epsilon = 0.01*cv.arcLength(contours[i],True)
        approx = cv.approxPolyDP(contours[i],epsilon,True)
        simplified_list.append(approx)

        print('lengths: plain contours {}; simplified {}'.format(len(contours[i]), len(approx)))

    # Draw contours + hull results
    drawing = np.zeros((canny_output.shape[0], canny_output.shape[1], 3), dtype=np.uint8)

    init_color = (255, 0, 0)
    for i in range(len(contours)):
        init_color = (init_color[0], init_color[1] + 10, init_color[2] + 10)
        color = (rng.randint(0,256), rng.randint(0,256), rng.randint(0,256))
        cv.drawContours(drawing, simplified_list, i, init_color, hierarchy=hier, maxLevel=1)
        # cv.drawContours(drawing, hull_list, i, init_color)

    # largest = sorted(hull_list, key=cv.contourArea, reverse=True)
    # cv.drawContours(drawing, largest, 0, (255,255,255))

    # Show in a window
    cv.imshow('Contours', drawing)
    cv.waitKey()

# Load source image
'''
parser = argparse.ArgumentParser(description='Code for Convex Hull tutorial.')
parser.add_argument('--input', help='Path to input image.', default='../data/stuff.jpg')
args = parser.parse_args()
src = cv.imread(args.input)
if src is None:
    print('Could not open or find the image:', args.input)
    exit(0)
'''
# Convert image to gray and blur it
src_gray = cv.cvtColor(src, cv.COLOR_BGR2GRAY)
src_gray = cv.blur(src_gray, (3,3))
# Create Window
source_window = 'Source'
cv.namedWindow(source_window)
cv.imshow(source_window, src)
max_thresh = 255
thresh = 100 # initial threshold
cv.createTrackbar('Canny thresh:', source_window, thresh, max_thresh, thresh_callback)
thresh_callback(thresh)
cv.waitKey()
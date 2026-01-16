import numpy as np 
import cv2
import os

from operator import itemgetter

import imageio
import time
import imutils
from collections import deque
import operator

import matplotlib.pyplot as plt
from pylab import rcParams




# ======================
# CV2 HELPER MODULES 
# ======================

def blur(image): 
    return cv2.medianBlur(image, 5)

def hsv(image): 
    return cv2.cvtColor(image, cv2.COLOR_RGB2HSV)

def hls(image): 
    return cv2.cvtColor(image, cv2.COLOR_BGR2HLS)

def lab (image):
    return cv2.cvtColor(image, cv2.COLOR_BGR2LAB)

def contrast(image): 
    lab= cv2.cvtColor(image, cv2.COLOR_BGR2LAB)
    l, a, b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=15.0, tileGridSize=(12,12))
    cl = clahe.apply(l)
    limg = cv2.merge((cl,a,b))
    final = cv2.cvtColor(limg, cv2.COLOR_LAB2BGR)
    return final

def isolate(image, mask): 
    image[mask == 0] = 0
    return image

# ======================
# PREPROCESSING MODULES 
# ======================

def masking(img, kern_size, lower, upper, er_iter, dil_iter):
    kernel = np.ones(kern_size,np.uint8)
    mask = cv2.inRange(img, lower, upper)
    mask = cv2.erode(mask, kernel, iterations = er_iter)
    mask = cv2.dilate(mask, kernel, iterations = dil_iter)
    return mask 

def thresholding(gray, min_pix, change, kernel_size1, kernel_size2, dir_iter, er_iter):
    ret, thresh = cv2.threshold(gray.copy(), min_pix, change, cv2.THRESH_BINARY) 
    kernel1 = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, kernel_size1)
    kernel2 = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, kernel_size2)
    dilation = cv2.dilate(cv2.bitwise_not(thresh), kernel1, iterations = dir_iter)  
    er = cv2.erode(dilation, kernel2, iterations = er_iter) 
    return er

def normalize_frame(frame):
    frame_yuv = cv2.cvtColor(frame, cv2.COLOR_BGR2YUV)
    channels = cv2.split(frame_yuv)
    channels[0] = cv2.equalizeHist(channels[0])
    frame_outpt = cv2.merge(channels)
    return cv2.cvtColor(frame_outpt, cv2.COLOR_YUV2BGR)

def preprocess_frame(frame, color, correct=False, show=False):
    frame = imutils.resize(cv2.cvtColor(frame[1], cv2.COLOR_BGR2RGB), width=600)
    r_frame = normalize_frame(frame)
    if color == 'red':
        er = get_processed_img_red(r_frame, correct)
    else:
        er = get_processed_img_white(frame, correct)
    if show: 
        plt.imshow(er)
        plt.show()
    return er

def get_processed_img_red(frame, correct):
    hsv_contr = hsv(blur(frame)) 
    if correct: 
        mask_red = masking(lab(frame), (3, 3), np.array([0, 150, 0]), np.array([255, 255, 80]), 0, 3)
    else: 
        mask_red = masking(lab(frame), (3, 3), np.array([0, 150, 0]), np.array([255, 255, 80]), 3, 5)
    frame_copy = frame.copy()
    frame_copy = isolate(frame_copy, mask_red)
    mask_red_1 = masking(lab(frame_copy), (0, 0), np.array([0, 150, 0]), np.array([255, 255, 80]), 0, 0)
    frame_copy = isolate(frame_copy, mask_red_1)
    gray = cv2.cvtColor(frame_copy, cv2.COLOR_BGR2GRAY)
    if correct: 
        er = thresholding(cv2.equalizeHist(gray), 95, 5, (2, 2), (2, 2), 3, 2)
    else: 
        er = thresholding(gray, 30, 5, (2, 2), (2, 2), 4, 3)
    er[er == 255] = 0
    return er


def get_processed_img_white(frame, correct):
    dst = cv2.edgePreservingFilter(frame, flags=1, sigma_s=60, sigma_r=0.8)
    mask_white = masking(hsv(dst).copy(), (3, 3), np.array([80, 0, 160]), np.array([210, 80, 250]), 4, 5)
    frame_copy = frame.copy()
    frame_copy[mask_white != 255] = 0
    gray = cv2.cvtColor(contrast(frame_copy), cv2.COLOR_BGR2GRAY)
    filt = np.array([[-1, -1, -1], [-1, 9, -1], [-1, -1, -1]])
    if correct: 
        er = thresholding(gray.copy(), 200, 10, (3, 3), (2, 2), 2,  2)
    else:
        er = thresholding(gray.copy(), 195, 150, (2, 2), (2, 2), 0,  0)
    return blur(er)


# ===========================
# OBJECT RECOGNITION MODULES 
# ===========================

def get_contours(img):
    cnts = cv2.findContours(cv2.Canny(img.copy(), 3, 5), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = imutils.grab_contours(cnts)
    return cnts

def get_radius(cont):
    ((x, y), radius) = cv2.minEnclosingCircle(cont)
    return radius

def get_center(cont):
    M = cv2.moments(cont)
    return (int(M["m10"] / M["m00"]), int(M["m01"] / M["m00"]))

def sort_centers(lst):
    return sorted(lst,key=itemgetter(0))

def get_sorted_ind(lst):
    return sorted(range(len(lst)),key=lst.__getitem__)

def get_list_centers(cnts):
    list_centers = []    
    for ind_c in range(len(cnts)):
        radius = get_radius(cnts[ind_c])
        if radius > 4:
            try:
                center = get_center(cnts[ind_c])
                if center[1] > 100 and center[1] < 170: 
                    list_centers.append(center)
            except: 
                continue
    return list_centers

def add_centers_to_list(list_centers, list_pts):
    list_centers_ind = get_sorted_ind(list_centers) 
    for ind in list_centers_ind:
        list_pts[list_centers_ind.index(ind)].append(list_centers[ind])
    return list_pts

def get_list_pts(frame, list_pts, color, correct, show=False):
    dict_col = {'red' : 6, 'white':12}
    er = preprocess_frame(frame, color, correct)
    cnts = get_contours(er)
    list_centers = get_list_centers(cnts)
    if len(list_centers) == dict_col[color]: 
        list_pts = add_centers_to_list(list_centers, list_pts)
    if show: 
        plt.imshow(er)
        plt.scatter([x[0] for x in list_centers], [y[1] for y in list_centers], color='r')
        plt.show()
    return list_centers, list_pts


# ======================
# OBJECT TRACKING MODULES 
# ======================

def calc_ds(list_pts):
    ds = []
    for deq in list_pts: 
        if len(deq) > 5: 
            deq.popleft()
        if len(deq) >= 3:
            dX = max([deq[len(deq)-1][0] - deq[i][0] for i in range(len(deq) - 2)])
            dY = max([deq[len(deq)-1][1] - deq[i][1] for i in range(len(deq) - 2)])
            if abs(dX > 40): 
                for i in range(len(deq)):
                    deq[i] = deq[len(deq)-1]
            ds.append(np.sqrt(dX**2 + dY**2))
    return ds


def check_ball_move(ds, dist_thres, balls_moved, list_centers, frame, prev_score, appended=False):
    if any(d >= dist_thres for d in ds):
        ind_ball = [i for i, d in enumerate(ds) if d >= dist_thres]
        if prev_score == None or prev_score != (ind_ball[0] + 1):
            balls_moved.append(ind_ball[0] + 1)
            appended = True
    #         print ('Ball number ', [i+ 1 for i in ind_ball]) # list_pts.index(deq))
            plt.imshow(imutils.resize(cv2.cvtColor(frame[1], cv2.COLOR_BGR2RGB), width=600))
            plt.scatter([x[0] for x in list_centers], [y[1] for y in list_centers], color='g')
            plt.show()
    return balls_moved, appended
# Author Branislav Vezilic
# Description: Detecting traffic lights using colour

import numpy as np
import cv2

def detect(img):
    '''
    :param img: image in BGR color space
    :return:
        dist - distance to traffic light
        img - image with detected traffic light
    '''
    # Convert BGR to HSV
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

    # Define range of red color in HSV
    lower_red1 = np.array([0, 100, 100])
    upper_red1 = np.array([9, 235, 235])

    lower_red2 = np.array([170, 100, 100])
    upper_red2 = np.array([179, 235, 235])

    # Threshold the HSV image to get only blue colors
    mask1 = cv2.inRange(hsv, lower_red1, upper_red1)
    mask2 = cv2.inRange(hsv, lower_red2, upper_red2)

    # Bitwise-OR of the masks
    mask = cv2.bitwise_or(mask1, mask2)

    # Initialize distance
    dist = -1

    # Finding contours in mask image
    img2, contours, hierarchy = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        area = cv2.contourArea(cnt)
        # Conditions that have to be satisfied for valid 'STOP' sign
        if 500 > area > 50 and x > mask.shape[1]/2 and abs(w-h)<=3:
            # Draw rectangle over 'STOP' sign
            cv2.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), 2)
            # Calculate distance to 'STOP' sign
            dist = distance(w)

    return dist, img


def distance(width_in_pix):
    '''
    :param width_in_pix: width of object in image
    :return: distance to traffic light
    '''
    known_distance = 3.5
    known_width = 1.0
    focal_length = get_focal_length(known_distance, known_width)
    return (known_width * focal_length) / width_in_pix


def get_focal_length(known_distance, known_width, width_in_pixels=80):
    return (width_in_pixels * known_distance) / known_width

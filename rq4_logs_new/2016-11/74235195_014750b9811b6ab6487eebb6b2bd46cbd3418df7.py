import os
from os import listdir
from os.path import isfile, join

import cv2
from theano.gradient import np

GROUND_TRUTH_DIR = '/media/b1/My Book/CSC420Data/data_road/training/gt_image_2/'
OUTPUT_DIR = '/media/b1/My Book/CSC420Data/data_road/training/gt_image_road_mask/'
GROUND_COLOUR = np.array((255, 0, 255))


all_files = [join(GROUND_TRUTH_DIR, f) for f in listdir(GROUND_TRUTH_DIR) if isfile(join(GROUND_TRUTH_DIR, f))]

os.makedirs(OUTPUT_DIR, exist_ok=True)


for i in all_files:
    img = cv2.imread(i)
    mask = cv2.inRange(img, GROUND_COLOUR, GROUND_COLOUR)
    cv2.imwrite(OUTPUT_DIR + os.sep + os.path.basename(i),mask)
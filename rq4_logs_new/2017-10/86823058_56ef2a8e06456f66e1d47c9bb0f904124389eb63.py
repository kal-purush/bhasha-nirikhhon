#!/usr/bin/env python3

'''
BulkImageFlipMirror.py v0.1.0
Python script for bulk flipping mirrored images in directory

Author:     Kim Eirik Kvassheim
URL:        https://github.com/kek91
License:    MIT
'''

from os import path, makedirs
from glob import glob
from PIL import Image, ImageOps

print("\nBulkImageRotater.py\n\
Rotates all images in selected folder 90 degrees clockwise.\n\
https://github.com/kek91/scripts\n")

IMAGES_SRC = input((r"Source directory: "))
IMAGES_DST = IMAGES_SRC + "\\Flipped"
EXTENSIONS = ["jpg", "jpeg", "png", "bmp", "tiff", "gif"]

print("\nProcessing files in " + IMAGES_SRC + ", please wait...\n")

INDEX_IMG = 0
INDEX_OTHER = 0

if not path.exists(IMAGES_DST):
    makedirs(IMAGES_DST)

for f in glob(IMAGES_SRC+"\\*.*", recursive=True):
    filename = path.basename(f)
    fileext = path.splitext(f)[1].lower().strip(".")
    if fileext in EXTENSIONS:
        try:
            img = Image.open(f)
            print("Flipping:\t", filename)
            img = ImageOps.mirror(img)
            img.save(IMAGES_DST+"\\"+filename)
            #img.rotate(-90, expand=True).save(IMAGES_DST+"\\"+filename)
            INDEX_IMG += 1
        except IOError:
            print("Skipping:\t", filename)
            INDEX_OTHER += 1
    else:
        print("Skipping:\t", filename)
        INDEX_OTHER += 1

print("\nBulk image flipper has finished. Results:\n\n\
Processed images:\t" + str(INDEX_IMG) + "\n\
Invalid files:\t\t" + str(INDEX_OTHER) + "\n\n\
Destination:\t\t" + IMAGES_DST + "\n\n")

keep_window_open = input()

import sys
sys.path.append('/home/arm/.local/lib/python3.7/site-packages')
import pytesseract
sys.path.append('ArmPi/')

import cv2
import time
import math
import logging
import numpy as np
# import pytesseract
from PIL import Image

def ocr_core(img):
    """
        This function 
    """
    text = pytesseract.image_to_string(Image.open(img))
    print("Text:", text)

if __name__ == '__main__':
    ocr_core('/home/arm/Documents/intro-to-robotics-2-project/arm/Sample Photos/rr.png')
    print("over")
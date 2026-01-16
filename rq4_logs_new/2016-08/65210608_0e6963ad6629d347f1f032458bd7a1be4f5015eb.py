# -*- coding: utf-8 -*-

"""Pattern recognition

This file provides functions to find well-defined patterns in an image
"""

import numpy as np
import cv2

def cornerPointsChess(img):
    """Find chessboard in image
    
    Args:
        img: An openCV image ndarray in a grayscale or color format.
    """
    NBR_COLUMNS = 7
    NBR_ROWS = 7
    
    gray = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY) 
    ret, corners = cv2.findChessboardCorners(gray, (NBR_COLUMNS,NBR_ROWS),None)
    
    if ret == True:
        print "Chessboard found!"
        x1 = round(corners[0][0][0])
        y1 = round(corners[0][0][1])
        x2 = round(corners[NBR_COLUMNS*NBR_ROWS-1][0][0])
        y2 = round(corners[NBR_COLUMNS*NBR_ROWS-1][0][1])
        print "Endpoints: ("+str(x1)+","+str(y1)+") ; ("+str(x1)+","+str(y1)+")"
    
        # Draw and display the corners (ADD FRAMES)
        cv2.drawChessboardCorners(img, (NBR_COLUMNS,NBR_ROWS), corners,ret)
        print "Print image"
        cv2.imshow('img',img)
        cv2.waitKey(1)
            
        return x1,y1,x2,y2
    else:
        print "Chessboard not found!"
        return -1,-1,-1,-1
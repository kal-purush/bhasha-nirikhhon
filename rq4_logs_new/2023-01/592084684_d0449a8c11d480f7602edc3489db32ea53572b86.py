# PARKING SPOT AVAILABILITY CHECKER

# This program determine if a selected parking spot is full or empty.
# The user selects a parking spot by clicking of the top-left corner of the said spot.

# the selected spot is then coveted to a threshold image and the number of pixels is counted,
# if the number of pixels is hight ( > 130) then the spot is most likely occupied. 

import cv2 as cv
import pickle as pikl
import cvzone as cvz

parkingLot = cv.imread('img/parking-lot.png') # open original image

grayImg = cv.cvtColor(parkingLot, cv.COLOR_BGR2GRAY) # covert to grayscale
blurImg = cv.GaussianBlur(grayImg, (3, 3), cv.BORDER_DEFAULT) # blur image, (int,int) = blur intensity
imgThreshold = cv.adaptiveThreshold(grayImg, 255, cv.ADAPTIVE_THRESH_GAUSSIAN_C, cv.THRESH_BINARY_INV, 25, 15) # convert image to threshold

width, height = 40, 15 # dimensions of selection rectangle


try:
    with open('posList', 'rb') as file: # try to open position list file from previous session 

        posList = pikl.load(file) # fill position list with previously selected spots
except:
    posList = []

def checkParkingSpace():
# crops selected spot and determines the pixel count,
# displays pixel count and occupancy status 

    numFullSpots = 0

    for pos in posList:

        x,y = pos

        imgCrop = imgThreshold[y:y+height, x:x+width]

        # cv.imshow(str(x * y), imgCrop)

        count = cv.countNonZero(imgCrop) # count number of pixels in selected spot

        cvz.putTextRect(parkingLot, str(count), (x, y+height-10), scale = 1, thickness=1, offset=0) # display pixel count

        if count > 130: # if parking spot is occupied
            cvz.putTextRect(parkingLot, 'FULL', (x+width+6, y+height), scale = 1, thickness=1, offset=0) # display FULL status
            numFullSpots += 1 

        availability = round(numFullSpots / len(posList) * 100, 2)

        cvz.putTextRect(parkingLot, str(availability) + '% FULL', (0, parkingLot.shape[0]), scale = 1, thickness=1, offset=0) 


def mouseClick ( events, x, y, flage, params):

    if events == cv.EVENT_LBUTTONDOWN: # add new spot selection to positions list
        posList.append((x, y))

    if events == cv.EVENT_RBUTTONDOWN: # remove spot from positions list

        for i, pos in enumerate(posList):

            clkX, clkY = pos
            if clkX < x < clkX + width and clkY < y< clkY + height: # if right mouse click is in the area of a selected spot

                posList.pop(i)

        
    with open('posList', 'wb') as file: # add updated position list to posList record file

        pikl.dump(posList, file)
                

                
while True:

    parkingLot = cv.imread('img/parking-lot.png') # update original image

    for pos in posList:

        checkParkingSpace()

        cv.rectangle(parkingLot, pos, (pos[0]+width, pos[1]+height), (255, 0, 233), 2) # draw rectangle around all selected spots

    cv.imshow('parking-lot', parkingLot) # show original image
    # cv.imshow('parking-lot', imgThreshold) # show threshold image

    cv.setMouseCallback('parking-lot', mouseClick) # mouse click event listener
    
    key = cv.waitKey(1)
    
    if key == ord(" "): # if key press is the space bar, end program

        break
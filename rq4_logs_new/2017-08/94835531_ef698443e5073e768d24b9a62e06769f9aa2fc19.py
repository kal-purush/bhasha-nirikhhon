import sys
import cv2,time,argparse,glob
import numpy as np
from naoqi import ALProxy
from time import sleep

#global variable to keep track of 
show = False

def onTrackbarActivity(x):
    global show
    show = True
    pass

if __name__ == '__main__' :
    if(len(sys.argv) <= 1):
        print "parameter error"
        print "python " + sys.argv[0] + " <ipaddr>"
        sys.exit()

    ip_addr = sys.argv[1]
    port_num = 9559

    # get NAOqi module proxy
    videoDevice = ALProxy('ALVideoDevice', ip_addr, port_num)
    # subscribe top camera
    AL_kTopCamera = 0
    AL_kQVGA = 1            # 320x240
    AL_kBGRColorSpace = 13
    captureDevice = videoDevice.subscribeCamera(
        "test", AL_kTopCamera, AL_kQVGA, AL_kBGRColorSpace, 10)

    # create image
    width = 320
    height = 240
    image = np.zeros((height, width, 3), np.uint8)

    # load the image
    # original = cv2.imread(files[0])
    # #Resize the image
    # rsize = 250
    # original = cv2.resize(original,(rsize,rsize))

    #creating windows to display images
    cv2.namedWindow('SelectHSV',cv2.WINDOW_AUTOSIZE)

    #creating trackbars to get values for HSV
    cv2.createTrackbar('HMin','SelectHSV',0,180,onTrackbarActivity)
    cv2.createTrackbar('HMax','SelectHSV',0,180,onTrackbarActivity)
    cv2.createTrackbar('SMin','SelectHSV',0,255,onTrackbarActivity)
    cv2.createTrackbar('SMax','SelectHSV',0,255,onTrackbarActivity)
    cv2.createTrackbar('VMin','SelectHSV',0,255,onTrackbarActivity)
    cv2.createTrackbar('VMax','SelectHSV',0,255,onTrackbarActivity)

    result = videoDevice.getImageRemote(captureDevice)
    values = map(ord, list(result[6]))
    i = 0
    for y in range(0, height):
        for x in range(0, width):
            image.itemset((y, x, 0), values[i + 0])
            image.itemset((y, x, 1), values[i + 1])
            image.itemset((y, x, 2), values[i + 2])
            i += 3
    # show image
    cv2.imshow("pepper-top-camera-320x240", image)
    # sleep(5)

    # show all images initially
    # cv2.imshow('SelectHSV',original)

    while(1):
        # videoCamera to mat
        # get image
        sleep(2)
        result = videoDevice.getImageRemote(captureDevice)
        if result == None:
            print 'cannot capture.'
        elif result[6] == None:
            print 'no image data string.'
        else:
            # translate value to mat
            values = map(ord, list(result[6]))
            i = 0
            for y in range(0, height):
                for x in range(0, width):
                    image.itemset((y, x, 0), values[i + 0])
                    image.itemset((y, x, 1), values[i + 1])
                    image.itemset((y, x, 2), values[i + 2])
                    i += 3
            original = image
            # show image
            # cv2.imshow("pepper-top-camera-320x240", image)

        k = cv2.waitKey(1) & 0xFF
        if k == 27:
            break
        
        if show: # If there is any event on the trackbar
            show = False

            # Get values from the HSV trackbar
            HMin = cv2.getTrackbarPos('HMin','SelectHSV')
            SMin = cv2.getTrackbarPos('SMin','SelectHSV')
            VMin = cv2.getTrackbarPos('VMin','SelectHSV')
            HMax = cv2.getTrackbarPos('HMax','SelectHSV')
            SMax = cv2.getTrackbarPos('SMax','SelectHSV')
            VMax = cv2.getTrackbarPos('VMax','SelectHSV')
            minHSV = np.array([HMin, SMin, VMin])
            maxHSV = np.array([HMax, SMax, VMax])

            # Convert the BGR image to other color spaces
            imageHSV = cv2.cvtColor(original,cv2.COLOR_BGR2HSV)

            # Create the mask using the min and max values obtained from 
            # trackbar and apply bitwise and operation to get the results                     
            maskHSV = cv2.inRange(imageHSV,minHSV,maxHSV)
            resultHSV = cv2.bitwise_and(original, original, mask = maskHSV)
            
            cv2.imshow('SelectHSV',resultHSV)
    cv2.destroyAllWindows()
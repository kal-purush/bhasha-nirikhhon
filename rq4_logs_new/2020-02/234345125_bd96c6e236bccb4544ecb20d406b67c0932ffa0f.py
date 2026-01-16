#!/usr/bin/python3

# mobileNetSSD.py
# Chris Wieringa, cwiering@umich.edu, 2020-02-28
# Michael Farmer, CSC548 Winter 2020 UM Flint
#
# Based heavily on code from Adrian Rosebrock, PhD from https://www.pyimagesearch.com/2017/09/11/object-detection-with-deep-learning-and-opencv/ using the Caffe deploy network found at https://github.com/chuanqi305/MobileNet-SSD

# # # # # #
# SETUP

import numpy as np
import cv2

class mobileNetSSD():
    ''' Runs the MobileNet-SSD Caffe network on an image, extracting "person" objects '''

    # initialize, accept the image and a boolean on whether or not to display when processed
    def __init__(self,image,confidence,show):
        self.image = image
        self.conf  = confidence
        self.show  = show
        self.processed = False
        self.numpersons = 0

    # process the image
    def process(self):
        ''' Runs the deep neural net on the image.  Returns the count of persons and the image '''

        # double-check to make sure I'm not already processed
        if self.processed: return

        # load the deep neural net into OpenCV
        model = 'supportfiles/MobileNetSSD_deploy.caffemodel'
        prototxt = 'supportfiles/MobileNetSSD_deploy.prototxt.txt'
        net = cv2.dnn.readNetFromCaffe(prototxt,model)

        # image specs
        (h, w) = self.image.shape[:2]

        # make the image blob
        blob = cv2.dnn.blobFromImage(self.image, 0.007843, (w, h), 127.5)

        # pass the blob through the network and run detections
        net.setInput(blob)
        detections = net.forward()

        # iterate over the detections
        for i in np.arange(0, detections.shape[2]):

            # extract confidence; continue if over threshold
            obj_confidence = detections[0,0,i,2]
            if obj_confidence > self.conf:

                # get the obj label index; i'm looking for person (15)
                index = int(detections[0,0,i,1])
                if index != 15: continue

                # calculate the detected object box, add to the processed image
                box = detections[0,0,i,3:7] * np.array([w,h,w,h])
                (x1,y1,x2,y2) = box.astype("int")
                label = "{:.2f}%".format(obj_confidence * 100)
                cv2.rectangle(self.image,(x1,y1),(x2,y2),[0,255,255],2)
                y = y1 - 15 if y1 - 15 > 15 else y1 + 15
                cv2.putText(self.image,label,(x1,y),
                        cv2.FONT_HERSHEY_SIMPLEX,0.5,[0,255,255],2)
        
                # add to count
                self.numpersons += 1
        
        # show the final image if set
        if self.show: 
            cv2.imshow("MobileNet-SSD",self.image)

        # set the processed variable
        self.processed = True
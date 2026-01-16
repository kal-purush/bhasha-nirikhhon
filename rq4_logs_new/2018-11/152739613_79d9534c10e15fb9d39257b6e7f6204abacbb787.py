#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from rosie_object_detector.srv import *
import rospy
from std_msgs.msg import Float32, Int32, String
import roslib
import numpy
import cv2
import sys
from std_msgs.msg import String
from sensor_msgs.msg import Image, CameraInfo
from cv_bridge import CvBridge, CvBridgeError
import os
from os import path
from pathlib import Path
import argparse
import sys
import time
import random
import numpy as np



class color_shape_detector:

	def __init__(self):
		self.bridge = CvBridge()
		self.success_color = False
		self.detected_color = ''

        #creating a filter
        #Position 0 is the lower limit and positon 1 the upper one
		lightgreen_threshold = np.array([[61,80,80],[81,255,255]])
		#red_threshold = np.array([[0, 80, 80],[20, 255, 255]])
		red_threshold = (np.array([[0, 98, 94],[2,241,255]]), np.array([[177,98,94],[180, 241, 255]]))
		blue_threshold = np.array([[95,80,80],[99, 255, 255]])
		orange_threshold = np.array([[8,97,127],[20,255,216]])
		yellow_threshold = np.array([[14,138,132],[19,247,255]])

		self.colors = {'Blue': blue_threshold, 'Red': red_threshold, 'LightGreen': lightgreen_threshold, 'Orange': orange_threshold, 'Yellow': yellow_threshold}
		self.shapes = {'Triangle' : 3, 'Square' : 4, 'Circle' : 15}

        #creating a message filter for synchronizing depth an color info
		self.image_sub = rospy.Subscriber("/camera/rgb/image_rect_color", Image, self.image_callback)

        #creating image kernels for morphological operations
		self.kernel_op = np.ones((5,5),np.uint8)
		self.kernel_cl = np.ones((5,5),np.uint8)

        # image size
		self.image_size = (640,480)

	def image_callback(self, image):
        #convertion from ROS Image format to opencv and filtering
		inImg = self.bridge.imgmsg_to_cv2(image,"passthrough")
		self.image = inImg
		#convertion from rgb to hsv
		inImg_hsv = cv2.cvtColor(self.image, cv2.COLOR_BGR2HSV)
		h,s,v = cv2.split(inImg_hsv)

		for color in self.colors:
			#appliying the color filter
			if (color == 'Red'):
				mask1 = cv2.inRange(inImg_hsv,self.colors[color][0][0], self.colors[color][0][1])
				mask2 = cv2.inRange(inImg_hsv,self.colors[color][1][0], self.colors[color][1][1])
				mask = cv2.bitwise_or(mask1,mask2)
			
			else:
				mask = cv2.inRange(inImg_hsv,self.colors[color][0], self.colors[color][1])

			

			cv2.imshow("mask", mask)


			#morphological transformation
			#kernel = np.ones((7,7),np.uint8)
			mask_op = cv2.morphologyEx(mask, cv2.MORPH_OPEN, self.kernel_op)
			mask_op_cl = cv2.morphologyEx(mask_op, cv2.MORPH_CLOSE, self.kernel_cl)
			#cv2.imshow("mask opening closing", mask_op_cl)


			#removing the small objects from the binary image
			im2,contours,h = cv2.findContours(mask_op_cl,cv2.RETR_TREE,cv2.CHAIN_APPROX_SIMPLE)
			mask_final = np.ones(mask_op_cl.shape[:2], dtype="uint8") * 255
			#cv2.imshow("middle step", mask_final)
			area_ev = 0
			iterator = 0
			biggest_area_index = 0

			if contours:
				for cnt in contours:
					area = cv2.contourArea(cnt)
					if (area > area_ev):
						area_ev = area
						biggest_area_index = iterator
						iterator = iterator + 1

				cnt =  contours[biggest_area_index]
				cv2.drawContours(mask_final, [cnt], -1, 0, -1)
				cv2.bitwise_not(mask_final,mask_final)
				cv2.imshow("Final mask", mask_final)
                #check if the color filer succeed
				if area_ev > 75:
					self.detected_color = color
					self.success_color = True
					print "The color detected is %s"%color
				cv2.waitKey(1)

def	color_detection():
    #class definition
	cd = color_shape_detector()
	#ros node defined
	rospy.init_node('color_shape_detector', anonymous = True )
	r = rospy.Rate(5) #5hz
	try:
		rospy.spin()
	except KeyboardInterrupt:
		print("Node shutting down")
	cv2.destroyAllWindows()

if __name__ == '__main__':
    color_detection()
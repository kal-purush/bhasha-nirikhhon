#!/usr/bin/env python

import rospy
from geometry_msgs.msg import Twist
import darknet_ros_msgs
from darknet_ros_msgs.msg import BoundingBoxes, BoundingBox, ObjectCount
from sensor_msgs.msg import Image
from apriltag_ros.msg import AprilTagDetectionArray
import cv2
from cv_bridge import CvBridge, CvBridgeError
import numpy as np
import message_filters
import sys
import os
from std_msgs.msg import String
from std_msgs.msg import Int16MultiArray


class Following:
    #480 * 640

    def __init__(self, object_names):
        self.center_x = 320
        self.center_y = 240
        self.turn_value = 0.25
        self.linear_value = 0.1
        self.linear_tol = 0.05
        self.linear_coefficient = 0.2
        self.angular_tol = 15
        self.angular_coefficient = 0.0015
        self.object_names = object_names
        self.rate = rospy.Rate(50)
        self.command = Twist()
        # set up publisher and subscriber
        self.xmin = 0
        self.xmax = 640
        self.ymin = 0
        self.ymax = 480
        self.sampling_depth = None
        self.control_pub = rospy.Publisher(
            'cmd_vel_mux/input/teleop', Twist, queue_size=0)
        self.april_center = rospy.Publisher(
            'tag_center', Int16MultiArray, queue_size=0)
        self.bridge = CvBridge()
        self.depth_msg = None

        # person following filters
        self.person_minimum_area = 13000

        # control buffers
        self.linear_velocity_buffer = 0
        self.linear_velocity_max = 0.3

        '''TODO: define callback function'''
        self.bounding_boxes_sub = rospy.Subscriber(
            '/darknet_ros/bounding_boxes', BoundingBoxes, self.bounding_boxes_callback)
        self.depth_callback = rospy.Subscriber(
            '/camera/depth_registered/image', Image, self.depth_callback)
        # self.detection_image_sub = rospy.Subscriber('/darknet_ros/detection_image', BoundingBoxes, self.bounding_boxes_callback)
        # self.depth_sub = rospy.Subscriber('/camera/depth_registered/image', Image, self.depth_callback)
        print('synchronized')

    # def depth_callback(self, image):
    #     self.image = image

    def bounding_boxes_callback(self, boundbox_msg):
        print('doing angular callback')
        data_test = Int16MultiArray()
        data_test.data = [380, 399, 380, 380, 380, 380, 380, 380]
        data_test.data =
        self.april_center.publish(data_test)
        # depth_msg = rospy.wait_for_message('/camera/depth_registered/image', Image)

        detected = False
        # if we are tracking person, we want the person with the biggest frame area
        if 'person' in self.object_names:
            best_area = float('-inf')
            for i in range(len(boundbox_msg.bounding_boxes)):
                oj = boundbox_msg.bounding_boxes[i]
                area = (oj.ymax - oj.ymin) * (oj.xmax - oj.xmin)
                if (oj.Class in self.object_names) and ((area > best_area) and area > self.person_minimum_area):
                    cur_frame = oj
                    detected = True
                    best_area = area
        else:
            for i in range(len(boundbox_msg.bounding_boxes)):
                if boundbox_msg.bounding_boxes[i].Class in self.object_names:
                    cur_frame = boundbox_msg.bounding_boxes[i]
                    detected = True
                    break
        if not detected:
            print('not detected')
            return
        # detected object in frame
        else:
            self.xmin = cur_frame.xmin
            self.ymin = cur_frame.ymin
            self.xmax = cur_frame.xmax
            self.ymax = cur_frame.ymax
            name = cur_frame.Class
            self.command = Twist()
            adjusted_angular = self.adjust_orientation(
                self.xmin, self.ymin, self.xmax, self.ymax)
            if self.sampling_depth != None:
                self.adjust_depth(self.sampling_depth)
                print(self.command.linear.x)
        if self.command.linear.x != 0:
            self.command.linear.x = min(
                (self.command.linear.x + self.linear_velocity_buffer)/2, self.linear_velocity_max)

        self.control_pub.publish(self.command)
        print('linear: ', self.command.linear.x)
        print('angular', self.command.angular.z)
        self.linear_velocity_buffer = self.command.linear.x

    def depth_callback(self, depth_msg):
        self.depth_msg = depth_msg
        cv_image = self.bridge.imgmsg_to_cv2(depth_msg, "32FC1")
        depth_matrix = np.array(cv_image)
        xmin = self.xmin
        xmax = self.xmax
        ymin = self.ymin
        ymax = self.ymax

        center = [xmin + (xmax - xmin)/2, ymin + (ymax - ymin)/2]
        chord = 5
        new_xmin = center[0] - chord
        new_xmax = center[0] + chord
        new_ymin = center[1] - chord
        new_ymax = center[1] + chord

        sampling_matrix = depth_matrix[new_ymin:new_ymax, new_xmin:new_xmax]
        sampling_depth = np.nanmean(sampling_matrix)

        self.sampling_depth = sampling_depth

    def adjust_orientation(self, xmin, ymin, xmax, ymax):
        # adjust the angular twist of the turtlebot to let object be in the center of the image
        xmid_point = xmin + (xmax - xmin)/2
        print('xmid', xmid_point)
        error = xmid_point - self.center_x
        if xmid_point > self.center_x and abs(xmid_point - self.center_x) > self.angular_tol:
            self.command.angular.z = -self.angular_coefficient * error
            return True
        elif xmid_point < self.center_x and abs(xmid_point - self.center_x) > self.angular_tol:
            self.command.angular.z = -self.angular_coefficient * error
            return True
        self.command.angular.z = 0

        return False

    def adjust_depth(self, sampling_depth):
        # adjust the depth to let image be of a fixed size
        target_depth = 0.3
        tol = self.linear_tol
        if sampling_depth > target_depth and abs(sampling_depth - target_depth) > tol:
            self.command.linear.x = self.linear_coefficient * \
                (sampling_depth - target_depth)
            return True
        self.command.linear.x = 0
        return False

    def not_detected_algo(self):
        # if not detected anything, then just circle
        self.command.angular.z = 0.3
        self.command.linear.x = 0
        rospy.sleep(1)


if __name__ == "__main__":

    #follow_object = [sys.argv[1]]
    rospy.init_node('following_node', anonymous=True)
    print('INITIALIZED NODE')
    try:
        following_turtlebot = Following('')

    except rospy.ROSInterruptException:
        print("exception")
        pass
    following_turtlebot.rate.sleep()
    rospy.spin()
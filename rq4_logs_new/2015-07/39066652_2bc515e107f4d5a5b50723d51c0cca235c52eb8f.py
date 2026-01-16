#!/usr/bin/env python
# -*- coding: utf-8 -*-

import rospy
from std_msgs.msg import Float64
from geometry_msgs.msg import PoseStamped
import numpy as np


def callback(msg, pub):
    pub.publish(np.sqrt([(msg.pose.position.x**2)+(msg.pose.position.y**2)]))


if __name__ == "__main__":
    rospy.init_node("distance_calc")
    pub = rospy.Publisher("/human_robot_distance", Float64, queue_size=1)
    rospy.Subscriber("/human/transformed", PoseStamped, callback, callback_args=pub)
    rospy.spin()
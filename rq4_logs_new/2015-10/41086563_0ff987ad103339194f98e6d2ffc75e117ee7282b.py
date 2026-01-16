#!/usr/bin/env python3
# -*- coding: utf-8 -*-

### H27 10 26

import rospy
from ros_robo15.msg import Gamepad_cmd

def callback(data):
    if (str(data.button_x) == 'True'):
        rospy.loginfo(rospy.get_caller_id() + " X TRI")
    if (str(data.button_a) == 'True'):
        rospy.loginfo(rospy.get_caller_id() + " A CENT")
    if (str(data.button_b) == 'True'):
        rospy.loginfo(rospy.get_caller_id() + " B SNIP")

def history_node():
    rospy.init_node('history_node', anonymous=False)

    rospy.Subscriber("/gamepad_cmd", Gamepad_cmd, callback)

    rospy.spin()

if __name__ == '__main__':
    try:
        history_node()
    except rospy.ROSInterruptException:
        pass
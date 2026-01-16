#!/usr/bin/env python
import rospy
from std_msgs.msg import String

def callback(data):
    listen_data = data.data
    print(listen_data)

def listener():
    print("ready to listen")
    rospy.Subscriber('pythonclass', String, callback)

    rospy.spin()

if __name__ == "__main__":
    rospy.init_node('listener', anonymous = True)
    listener()
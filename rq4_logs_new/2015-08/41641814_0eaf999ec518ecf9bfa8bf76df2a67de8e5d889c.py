#!/usr/bin/env python

import roslib
import rospy
import tf
from tf.msg import tfMessage
from geometry_msgs.msg import TransformStamped
import tfx
import pickle
import sys

class RobotBroadcaster:
    def __init__(self, arm):
        print("init")
        self.arm = arm

    def broadcast_transform(self):
        rospy.init_node('robot_broadcaster', anonymous=True)
        self.pub = rospy.Publisher("/tf", tfMessage, queue_size=1, latch=True)

        f = open("/home/davinci2/catkin_ws/src/davinci_vision/launch/BC_registration/robot_transform_" + self.arm + \
            ".p", "r")
        p = pickle.load(f)
        f.close()

        pt = p.translation
        rot = p.rotation
        print("x: " + str(pt.x))
        print("y: " + str(pt.y))
        print("z: " + str(pt.z))
        print("x: " + str(rot.x))
        print("y: " + str(rot.y))
        print("z: " + str(rot.z))
        print("w: " + str(rot.w))
        
        msg = TransformStamped()

        msg.header.stamp = rospy.Time.now()
        msg.transform.rotation.x = rot.x
        msg.transform.rotation.y = rot.y
        msg.transform.rotation.z = rot.z
        msg.transform.rotation.w = rot.w
        msg.child_frame_id = "registration_brick"   
        msg.transform.translation.x = pt.x
        msg.transform.translation.y = pt.y
        msg.transform.translation.z = pt.z
        if self.arm == 'left':
            msg.header.frame_id = "one_remote_center_link"
        else:
            msg.header.frame_id = "two_remote_center_link"
            # ???
        while not rospy.is_shutdown():
                msg.header.stamp = rospy.Time.now()
                self.pub.publish([msg])
                rospy.sleep(0.5)
        
if __name__ == '__main__':
    r = RobotBroadcaster(sys.argv[1])
    r.broadcast_transform()
#!/usr/bin/env python

import time
import rospy
from std_msgs.msg import String
from adafruit_extended_bus import ExtendedI2C as I2C
from clare_arms.msg import ArmMovement
from adafruit_servokit import ServoKit


class ArmsController(object):
    def __init__(self):
        super(ArmsController, self).__init__()

        rospy.init_node("clare_arms", anonymous=False, disable_signals=False)
        i2c_bus = rospy.get_param("~i2c_bus", 3)

        # Setup servo connection
        i2c = I2C(i2c_bus)
        self._servokit = ServoKit(channels=16, i2c=i2c)

        self._arm_sub = rospy.Subscriber("clare/arms", ArmMovement, self._arm_callback)

    def set_shoulder_pos(self, pos_left, pos_right):
        if (0 <= pos_left <= 180) and (0 <= pos_right <= 180):
            # Make servo's rotate in the same direction (mirror image mount)
            self._servokit.servo[0].angle = pos_right
            self._servokit.servo[1].angle = 180 - pos_left
        else:
            rospy.logerr(f"Invalid shoulder position commanded, left: {pos_left} right: {pos_right}")

    def _arm_callback(self, msg):
        self.set_shoulder_pos(msg.shoulder_left_angle, msg.shoulder_right_angle)

    def run(self):
        rospy.logdebug('Arms node ready and listening')
        rospy.spin()

if __name__ == "__main__":
  
    n = ArmsController()
    try:
        n.run()
    except rospy.ROSInterruptException:
        pass
#!/usr/bin/env python
import rospy

from std_msgs.msg import Int64


def nilai_alas():
    rospy.init_node('alas')
    alas_pub = rospy.Publisher('alas_pub', Int64, queue_size=10)
    rate = rospy.Rate(1000)

    while not rospy.is_shutdown():
        alas = 10

        alas_pub.publish(alas)
        rate.sleep()


if __name__ == '__main__':
    try:
        nilai_alas()
    except rospy.ROSInterruptException:
        pass
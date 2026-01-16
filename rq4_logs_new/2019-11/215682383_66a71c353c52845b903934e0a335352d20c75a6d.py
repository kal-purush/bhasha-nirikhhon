import rospy
from std_msgs.msg import Int64

class depthSub:
    
    def __init__(self, myDepth):
        self.setDepth(myDepth)

    def setDepth(self, value):
        self.depth = int(value)

    def getDepth(self):
        return self.depth

    def talker(self):
        pub = rospy.Publisher('depthChatter',Int64,queue_size=10)

        rospy.init_node('depthTalker', anonymous=True)
        rate = rospy.Rate(10)

        pub.publish(self.getDepth())
        rospy.loginfo('Current Depth: %d' %self.getDepth() )
        rate.sleep()


# if __name__ == '__main__':
#     try:
#         talker()
#     except rospy.ROSInterruptException:
#         pass
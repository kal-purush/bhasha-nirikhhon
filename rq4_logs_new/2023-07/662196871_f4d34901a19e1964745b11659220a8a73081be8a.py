#!/usr/bin/env python3

import rospy
from tf2_msgs.msg import TFMessage
from rosgraph_msgs.msg import Clock
from threading import Lock

class TfRepublisherClock:

    def __init__(self):
        rospy.init_node("tf_republisher_clock", anonymous=True)
        
        old_tf_topic = rospy.get_param('~old_tf_topic', '/tf_wall_time')
        new_tf_topic = rospy.get_param('~new_tf_topic', '/tf')

        self._sub_tf = rospy.Subscriber(old_tf_topic, TFMessage, self._tf_cb)
        self._sub_clock = rospy.Subscriber('/clock', Clock, self._clock_cb)
        self._pub = rospy.Publisher(new_tf_topic, TFMessage, queue_size=10)

        self._lock_clock = Lock()
        self._current_time = rospy.Time.now()
        self._lock_tfs = Lock()
        self._current_tfs = TFMessage()

    def _clock_cb(self, clock_msg):
        
        with self._lock_clock:
            self._current_time = rospy.Time(clock_msg.clock.secs, clock_msg.clock.nsecs)

    def _tf_cb(self, tf_msg):

        with self._lock_tfs:
            self._current_tfs = tf_msg

    def run(self):
        # Get locks
        self._lock_clock.acquire()
        self._lock_tfs.acquire()

        # Update time stamp to the one from the clock topic
        for transform in self._current_tfs.transforms:
            transform.header.stamp = self._current_time
        self._pub.publish(self._current_tfs)

        # Release locks
        self._lock_clock.release()
        self._lock_tfs.release()

def main():

    node = TfRepublisherClock()
    rate = rospy.Rate(50)
    while not rospy.is_shutdown():
        node.run()
        rate.sleep()

if __name__ == "__main__":

    try:
        main()
    except rospy.ROSException as e:
        rospy.logerr("Something went wrong. %s", str(e))
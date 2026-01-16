#!/usr/bin/env python2

import rospkg
import rospy
import tf
import tf2_ros
#import geometry_msgs.msg
import sensor_msgs.msg

waypoints_file_path = rospkg.RosPack().get_path('follow_waypoints')+"/saved_path/pose.csv"
points_file_path = rospkg.RosPack().get_path('follow_waypoints')+"/saved_path/point.csv"

if __name__ == '__main__':
  rospy.init_node('utm_waypoint_manager', anonymous=True)
  
  rospack = rospkg.RosPack()
  listener = tf.TransformListener()
  
  print("Modes:\n\t3 = Record waypoint using tf(map->base_link)\n\t4 = Record waypoint using tf(map->base_footprint)")
  mode = raw_input("Mode[3/4]: ")
  if mode == "4":
    # Recording waypoints by tf(map->base_footprint)
    mode = 4
  elif mode == "3":
    # Recording waypoints by tf(map->base_link)
    mode = 3
  else:
    mode = 3
  
  
  rate = rospy.Rate(10.0)
  with open(waypoints_file_path, 'w') as poseFile, open(points_file_path, 'w') as pointFile:
    while not rospy.is_shutdown():
      command = raw_input("Command: ").rstrip()
      if command.lower() == "q":
        break
      elif command.lower() == "map" or command == "!3":
        print("Switched to map(base_link) mode")
        mode = 3
        continue
      elif command.lower() == "map" or command == "!4":
        print("Switched to map(base_footprint) mode")
        mode = 4
        continue
      elif command != "":
        poseFile.write(command + "\n")
        pointFile.write(command + "\n")
        rospy.loginfo("Added location: dock " + command.rstrip())
        continue
      
      trans = [0.0, 0.0, 0.0]
      rot = [0.0, 0.0, 0.0, 1.0]
      if mode == 3:
        try:
          now = rospy.Time(0)
          EARTH_FRAME = 'map'
          ROBOT_FRAME = 'base_link'
          listener.waitForTransform(EARTH_FRAME, ROBOT_FRAME, now, rospy.Duration.from_sec(1.0))
          trans, rot = listener.lookupTransform(EARTH_FRAME, ROBOT_FRAME, now)
        except (tf.LookupException, tf.ConnectivityException, tf.ExtrapolationException):
          rospy.logwarn("No transform from " + EARTH_FRAME + " to " + ROBOT_FRAME + " available")
          continue
        except tf2_ros.TransformException as e:
          rospy.logwarn("No transform: "+ str(e))
          continue
      elif mode == 4:
        try:
          now = rospy.Time(0)
          EARTH_FRAME = 'map'
          ROBOT_FRAME = 'base_footprint'
          listener.waitForTransform(EARTH_FRAME, ROBOT_FRAME, now, rospy.Duration.from_sec(1.0))
          trans, rot = listener.lookupTransform(EARTH_FRAME, ROBOT_FRAME, now)
        except (tf.LookupException, tf.ConnectivityException, tf.ExtrapolationException):
          rospy.logwarn("No transform from " + EARTH_FRAME + " to " + ROBOT_FRAME + " available")
          continue
        except tf2_ros.TransformException as e:
          rospy.logwarn("No transform: "+ str(e))
          continue
      
      line = "{},{},{},{},{},{},{}\n".format(trans[0], trans[1], trans[2], rot[0], rot[1], rot[2], rot[3])
      poseFile.write(line)
      pointFile.write(line)
      rospy.loginfo("Added location: " + line.rstrip())
      
      rate.sleep()
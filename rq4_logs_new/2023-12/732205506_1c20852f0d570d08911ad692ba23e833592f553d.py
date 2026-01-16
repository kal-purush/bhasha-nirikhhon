import math
import rospy
from open_manipulator_msgs.srv import *
from open_manipulator_msgs.msg import *

class Open_manipulator_x:

    def __init__(self):
            # Service Initialization
            self.service_forward_kinematics = "/goal_joint_space_path"

    def Inverse_kinematics(self,xe,ye,ze,alpha):

        a2 = 0.13
        a3 = 0.124
        a4 = 0.130
        d1 = 0.077

        q0 = math.atan2(ye, xe)

        r4 = math.sqrt(xe**2 + ye**2)
        r3 = r4 - a4 * math.cos(alpha)
        z3 = [z + a4 * math.sin(alpha) for z in ze]
        D = (a2**2 + a3**2 - r3**2 - (z3[0] - d1)**2) / (2 * a2 * a3)

        q2 = math.pi / 2 - math.acos(D)

        r2 = r3 - a3 * math.cos(q2)
        z2 = [z + a3 * math.sin(q2) for z in z3]

        q1 = math.atan2(r2, z2[0])
        q3 = q1 + alpha - q2 - math.pi / 2

        if abs(q0)>math.pi/2 & abs(q2) >math.pi/2 & abs(q2) >1.35 & abs(q3)>math.pi/2 & isinstance(q0+q1+q2+q3, (int, float)):
            print('Achievable position')

            return q0 , q1 , q2 , q3
        else:
            print('Unreachable position')

            return 0
    
    def perform_MGD(self, joint_angles, path_time):
        # Function utilizing "/goal_joint_space_path" service for robot motion planning based on MGD (Modified Geometric Description).
        # Inputs: joint_angles (list of radians), path_time (time in seconds).
        # Output: is_planned (boolean) indicating motor reachability.
        # Wait for service response.
         
        rospy.wait_for_service(self.service_name_MGD)
        try:
            # Create a function to call the service
            joints_service = rospy.ServiceProxy(self.service_forward_kinematics, SetJointPosition)
            
            # Create a message to send to the service
            request_message = SetJointPositionRequest()
            
            # Specify the joint angles to reach
            request_message.joint_position.position = joint_angles
            
            # Specify the time to reach the joint angles
            request_message.path_time = path_time
            
            # Specify which joint angle corresponds to which motor
            request_message.joint_position.joint_name = ['joint1', 'joint2', 'joint3', 'joint4']
            
            # Call the service
            response = joints_service(request_message)
            return response
            
        # In case of an error, return "False" to indicate that the command generation has failed
        except rospy.ServiceException as e:
            print("Service call failed: %s" % e)
            return False
 
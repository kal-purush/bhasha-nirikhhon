#!/usr/bin/env python
#

from __future__ import print_function
import rospy
from sensor_msgs.msg import JointState

from markers import *
from p_functions import *


# Initialize the node
rospy.init_node("testKineControlPosition")
print('starting motion ... ')
# Publisher: publish to the joint_states topic
pub = rospy.Publisher('joint_states', JointState, queue_size=10)

# Files for the logs
fxcurrent = open("/tmp/xcurrent.txt", "w")                
fxdesired = open("/tmp/xdesired.txt", "w")
fq = open("/tmp/q.txt", "w")

# Markers for the current and desired positions
bmarker_current  = BallMarker(color['RED'])
bmarker_desired = BallMarker(color['GREEN'])

# Joint names
jnames = ['joint_0', 'joint_1', 'joint_2',
          'joint_3', 'joint_4', 'joint_5', 'joint_6']

# Desired position
xd = np.array([0.5,0.4, 0.6])
# Initial configuration
q0  = np.array([0, 0, 0,0, 0, 0, 0])

# Resulting initial position (end effector with respect to the base link)
T = fkine_iiwa14(q0)
x0 = T[0:3,3]

# Markers for the current and the desired pose
bmarker_current.xyz(x0)
bmarker_desired.xyz(xd)

# Instance of the JointState message
jstate = JointState()
# Values of the message
jstate.header.stamp = rospy.Time.now()
jstate.name = jnames
# Add the head joint value (with value 0) to the joints
jstate.position = q0

# Frequency (in Hz) and control period 
freq = 200
dt = 1.0/freq
rate = rospy.Rate(freq)

# Initial joint configuration
q = copy(q0)
# Main loop
while not rospy.is_shutdown():
    # Current time (needed for ROS)
    jstate.header.stamp = rospy.Time.now()
    # Kinematic control law for position (complete here)
    # -----------------------------
    # Jacobiano para la configuracion actual q
    J = jacobian_iiwa14(q, 0.0001)
    # Cinematica directa dada la configuracion actual q
    x_i = fkine_iiwa14(q)
    x = x_i[0:3,3]
    # Error
    e = x-xd
    print(e)
    k=1
    # Derivada del error
    de = -k*e
    # Variacion de la configuracion articular
    Ja= np.dot(J, J.T)
    Jb= np.linalg.pinv(Ja)
    Jf= np.dot(J.T, Jb)
    dq = np.dot(Jf, de)
    # Integracion para obtener la nueva configuracion articular
    q = q + dt*dq
    # -----------------------------
    
    if(q[0] > 2.93):
        q[0] = 2.93
    elif(q[1] > 2.06):
        q[1] = 2.06
    elif(q[2] > 2.93):
        q[2] = 2.93
    elif(q[3] > 2.06):
        q[3] = 2.06
    elif(q[4] > 2.93):
        q[4] = 2.93
    elif(q[5] > 2.06):
        q[5] = 2.06
    elif(q[6] > 3.02):
        q[6] = 3.02
    elif(q[0] < -2.93):
        q[0] = -2.93
    elif(q[1] < -2.06):
        q[1] = -2.06
    elif(q[2] < -2.93):
        q[2] = -2.93
    elif(q[3] < -2.06):
        q[3] = -2.06
    elif(q[4] < -2.93):
        q[4] = -2.93
    elif(q[5] < -2.06):
        q[5] = -2.06
    elif(q[6] < -3.02):
        q[6] = -3.02

    # Log values                                                      
    fxcurrent.write(str(x[0])+' '+str(x[1]) +' '+str(x[2])+'\n')
    fxdesired.write(str(xd[0])+' '+str(xd[1])+' '+str(xd[2])+'\n')
    fq.write(str(q[0])+" "+str(q[1])+" "+str(q[2])+" "+str(q[3])+" "+
             str(q[4])+" "+str(q[5])+"\n")
    
    # Publish the message
    jstate.position = q
    pub.publish(jstate)
    bmarker_desired.xyz(xd)
    bmarker_current.xyz(x)
    # Wait for the next iteration
    rate.sleep()

print('ending motion ...')
fxcurrent.close()
fxdesired.close()
fq.close()
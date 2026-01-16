#!/usr/bin/python
# Keeps 0 phase difference between legs for normal walking
# Try to move one leg suddenly and fastly to turn.
import rospy
from geometry_msgs.msg import Pose, Vector3
from std_msgs.msg import Int32
import numpy as np
import time
import tf

ticks_per_rev = 3*298
old_l_data, old_r_data = 0, 0


def threshold(val, lb, ub):
        if val < lb:
                return lb
        if val > ub:
                return ub
        return val

def l_enc_callback(msg):
        global l_count, old_l_data
        l_count += abs(msg.data - old_l_data)
	old_l_data = msg.data

def r_enc_callback(msg):
        global r_count, old_r_data
        r_count += abs(msg.data - old_r_data)
	old_r_data = msg.data

def update_phase(time):
        #print(l_count)
        global desired_phi_l, desired_phi_r
        # Compute Speed
        actual_phi_l = abs(l_count) * 1.0 / ticks_per_rev
        actual_phi_r = abs(r_count) * 1.0 / ticks_per_rev

        # Compute left/right errors
        global ei_l, ei_r, old_ep_l, old_ep_r
        ep_l, ep_r = desired_phi_l - actual_phi_l, desired_phi_r - actual_phi_r
        ei_l = threshold(ei_l + ep_l, -15, 15)
        ei_r = threshold(ei_r + ep_r, -15, 15)
        ed_l, ed_r = ep_l - old_ep_l, ep_r - old_ep_r
        old_ep_l, old_ep_r = ep_l, ep_r

        # PID control of speed
        pwm_l = kp_l * ep_l + ki_l * ei_l + kd_l * ed_l
        pwm_r = kp_r * ep_r + ki_r * ei_r + kd_r * ed_r
        pwm_l = threshold(pwm_l, 0, 1)
        pwm_r = threshold(pwm_r, 0, 1)
        if False:
                print('ep_l, ep_r:', ep_l, ep_r);
                print('ei_l, ei_r:', ei_l, ei_r);
                print('phi_l, phi_r:', l_count, r_count);
                print('pwm:', pwm_l, pwm_r);
        control_pub.publish(Vector3(pwm_r, pwm_l, 0))

old_l_count, old_r_count = 0, 0
def update_vel(time):
        global old_l_count, old_r_count
        # Compute Speed
        actual_w_l = abs(l_count - old_l_count) * 1.0 / ticks_per_rev / dt
        actual_w_r = abs(r_count - old_r_count) * 1.0 / ticks_per_rev / dt
	old_l_count, old_r_count = l_count, r_count

	# Coordinate phase if not doing diff drive
	phase_l, phase_r = l_count % ticks_per_rev, r_count % ticks_per_rev
	phase_diff = (phase_r - phase_l) * 1.0 / ticks_per_rev
		 
	global ei_l, ei_r, old_ep_l, old_ep_r
        ep_l, ep_r = desired_w_l - actual_w_l, desired_w_r - actual_w_r
	# Include phase error if not doing differential drive
	if not diff_drive:
		ep_l += phase_diff / 2
		ep_r -= phase_diff / 2
		
        ei_l = threshold(ei_l + ep_l, -15, 15)
        ei_r = threshold(ei_r + ep_r, -15, 15)
        ed_l, ed_r = ep_l - old_ep_l, ep_r - old_ep_r
        old_ep_l, old_ep_r = ep_l, ep_r
        
        # PID control of speed
        pwm_l = kp_l * ep_l + ki_l * ei_l + kd_l * ed_l
        pwm_r = kp_r * ep_r + ki_r * ei_r + kd_r * ed_r
        pwm_l = threshold(pwm_l, 0, 1)
        pwm_r = threshold(pwm_r, 0, 1)
                
	if False:
		print('ep_l, ep_r:', ep_l, ep_r);
                print('ei_l, ei_r:', ei_l, ei_r);
                print('phi_l, phi_r:', l_count, r_count);
                print('pwm:', pwm_l, pwm_r);
        control_pub.publish(Vector3(pwm_r, pwm_l, 0))

kp_diff_r = 20 
diff_r_i = 0
ki_diff_r = 0 
kp_diff_theta = 0.1
diff_drive = False
def control_callback(msg):
	global desired_w_l, desired_w_r, diff_r_i
	diff_r = np.sqrt(msg.position.x**2 + msg.position.y ** 2)
        quaternion = (msg.orientation.x, msg.orientation.y, msg.orientation.z, msg.orientation.w)
        euler = tf.transformations.euler_from_quaternion(quaternion)
        actual_yaw = euler[2]
        desired_yaw = np.arctan2(-msg.position.y, -msg.position.x)

        diff_theta = (desired_yaw - actual_yaw) / np.pi * 180
	diff_r_i += diff_r;
	
	desired_w_l = kp_diff_r * diff_r + ki_diff_r * diff_r_i;
	desired_w_r = kp_diff_r * diff_r + ki_diff_r * diff_r_i;
	if (msg.position.y > 0.05):
		desired_w_l, desired_w_r = 0, 0
		diff_r_i = 0
	if (abs(diff_theta) > 30) and (msg.position.y < -0.03):
		diff_drive = True
		desired_w_l += kp_diff_theta * diff_theta
		desired_w_r -= kp_diff_theta * diff_theta
	else:
		diff_drive = False	
        
	print('diff_r, diff_theta:', diff_r, diff_theta) 
        print('desired_w_l, desired_w_r, diff_drive:', desired_w_l, desired_w_r, diff_drive)	

desired_w_l, desired_w_r = 0, 0;
desired_phi_l, desired_phi_r = 0, 0;
ei_l, ei_r, old_ep_l, old_ep_r = 0, 0, 0, 0
l_count, r_count = 0, 0
kp_l, ki_l, kd_l = 1.2, 0, 0.2
kp_r, ki_r, kd_r = 1.2, 0, 0.2

rospy.init_node("phase_control", anonymous=True)
rospy.Subscriber("/l_enc", Int32, l_enc_callback)
rospy.Subscriber("/r_enc", Int32, r_enc_callback)
control_pub = rospy.Publisher('/cmd_pwm', Vector3, queue_size = 1)

dt = 0.1
calibration_done = False
timer = rospy.Timer(rospy.Duration(dt), update_phase)

print('use a/d to calibrate legs so that they are in phase, press z to stop')
cmd = raw_input()
while (cmd != 'z'):
        if (cmd == 'a'):
                desired_phi_l += 0.05
        if (cmd == 'd'):
                desired_phi_r += 0.05
        cmd = raw_input()

timer.shutdown()
rospy.sleep(1)
print('calibration Done, put OpenRoACH on the treadmill and press enter!')

rospy.Subscriber("/Robot_1/pose", Pose, control_callback)
calibration_done = True
ei_l, ei_r, old_ep_l, old_ep_r = 0, 0, 0, 0
l_count, r_count = 0, 0
kp_l, ki_l, kd_l = 0.6, 0.015, 0.2
kp_r, ki_r, kd_r = 0.6, 0.015, 0.2
jiggle_l, jiggle_r = 0, 0

timer = rospy.Timer(rospy.Duration(dt), update_vel)
rospy.spin()
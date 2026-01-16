# Model and simulate a dual-arm mobile manipulator
# 
# Arms:        Franka Research 3 (FR3) by Franka Emika
# Mobile base: Bunker by Agilex
#
# Model information is contained in the XML model files.
#
# Requires MuJoCo and OpenGL
# 
# Verion 1.0  - 2025.02.03 
#
''' Copyright (c) 2025 Miguel Torres-Torriti under MIT License. See LICENSE.txt file '''
''' Source: '''

import mujoco
from mujoco.glfw import glfw
import OpenGL.GL as gl
import numpy as np

# --- Model file ---
xml_path = "DAMM1.xml"
#xml_path = "DAMM_FR3_arms_rear.xml"
#xml_path = "DAMM_FR3_arms_centerline.xml"

# --- MuJoCo data structures: model, camera, visualization options ---
model = mujoco.MjModel.from_xml_path(xml_path)
data = mujoco.MjData(model)
cam = mujoco.MjvCamera()
opt = mujoco.MjvOption()

# Set camera configuration
cam.azimuth = 89.608063
cam.elevation = -11.588379
cam.distance = 5.0
cam.lookat = np.array([0.0, 0.0, 1.5])

# Initialize visualization data structures
mujoco.mjv_defaultOption(opt)
scene = mujoco.MjvScene(model, maxgeom=10000)
mujoco.mj_forward(model, data)

# --- MuJoCo actuator options ---
model.actuator_gainprm[0, 0] = 1 # Set gain of torque actuator at right-rear   wheel
model.actuator_gainprm[1, 0] = 1 # Set gain of torque actuator at right-center wheel
model.actuator_gainprm[2, 0] = 1 # Set gain of torque actuator at right-front  wheel
model.actuator_gainprm[3, 0] = 1 # Set gain of torque actuator at left-rear   wheel
model.actuator_gainprm[4, 0] = 1 # Set gain of torque actuator at left-center wheel
model.actuator_gainprm[5, 0] = 1 # Set gain of torque actuator at left-front  wheel

# --- Controller variables ---

# -- Set-points --
# q_ref_base: Set the reference pose of the mobile base [x^w, y^w, theta_z^w] in world coordinates
# q_ref_arm_1: Set the reference position of the joints of the first FR3 arm
# q_ref_arm_2: Set the reference position of the joints of the first FR3 arm

q_ref_base = np.array([3.0, 2.0, 0]) 
q_ref_arm_1 = np.array([ np.pi/2, 0.0, 0.0, -np.pi/2, 0.0, np.pi/2, np.pi/4])  
q_ref_arm_2 = np.array([-np.pi/2, 0.0, 0.0, -np.pi/2, 0.0, np.pi/2, np.pi/4])  

# ***** Mobile base controller *****
#
# The mobile base motion controller is designed a velocity control loop (PID), 
# nested within a position/heading control loop (P).  This means that:
# 1. Given a position/heading error, desired longitudinal/angular velocities are computed.
# 2. The desired longitudinal/angular velocities are the manipulated variabes (MV)
#    of the position control loop and the set-points (SPs) of the velocity control loop.
# 3. Given a longitudinal/angular velocity error, the desired longitudinal/angular
#    accelerations are computed.
# 4. Using the robot's motion dynamics model, the longitudinal/angular accelerations
#    are tranformed into right and left wheel torques.

# -- Mobile base parameters (these are approximated parameters).
# -- The mobile  controller has not been optimized.
W = 0.778            # Body width
L = 1.023            # Body length
m = 150+70           # Robot mass
J = m*(L**2+W**2)/12 # Robot inertia moment about Z^b-axis
c = 1                # Longitudinal viscous friction
b = 1                # Rotational viscous friction
r = 0.2              # Wheel radius


# Desired velocity, acceleration and torques
Vel_lin_des = 0.0   # Desired linear  velocity
Vel_ang_des = 0.0   # Desired angular velocity
Accel_lin_des = 0.0 # Desired linear  acceleration
Accel_ang_des = 0.0 # Desired angular acceleration
Torque_R = 0.0      # Torque of right wheels (total)
Torque_L = 0.0      # Torque of left  wheels (total)

Ts = model.opt.timestep  # Sampling time

# Automatic control-mode variables
e_vel = 0.0         # Linear velocity error at time k 
e_vel1 = 0.0        # Linear velocity error at time k-1
e_vel2 = 0.0        # Linear velocity error at time k-2
e_omega = 0.0       # Angular velocity error at time k 
e_omega1 = 0.0      # Angular velocity error at time k-1
e_omega2 = 0.0      # Angular velocity error at time k-2

# Mobile base postion/heading P controller parameters
Kp_pos  = 0.3
Kp_head = 9.0

# Mobile base velocity PID controller parameters
Kp_vel = 500.0
Ki_vel = 10.05
Kd_vel = 20.01
Kp_omega = 590.0
Ki_omega = 10.05
Kd_omega = 20.1

# ***** Arms controllers *****
#
# The arms controllers are designed as PD controllers for the joint angles.
# For simplicity of exposition, all joints are assumed to have the same controller
# parameters. The arm controllers have not been optimized.
# 
Kp_arm = 200.0
Kd_arm =  20.0

def base_controller(model, data):
    global Accel_lin_des, Accel_ang_des
    global Vel_lin_des, Vel_ang_des
    global Pos_x_des, Pos_y_des
    global Torque_R, Torque_L, Ts
    global e_vel, e_vel1, e_vel2, e_omega, e_omega1, e_omega2

    # Position P-controller loop
    Pos_x_des, Pos_y_des = q_ref_base[0], q_ref_base[1] # Mobile base reference position
    x, y  = data.qpos[0], data.qpos[1]                  # Mobile base current position
    e_dist = np.sqrt((Pos_x_des-x)**2 + (Pos_y_des-y)**2) # Mobile base position error
    theta  = quat2euler(data.qpos[3:7])[2]              # Mobile base orientation
    theta_des = np.arctan2(Pos_y_des-y, Pos_x_des-x)    # Direction of reference position
    e_theta = theta_des - theta                           # Mobile base heading error 

    print("Distance and heading errors: ", np.round(e_dist,4), 
                                           np.round(180.0*e_theta/np.pi,4))
          
    Vel_lin_des = Kp_pos*e_dist   # Set desired linear velocity proportional to the position error
    Vel_ang_des = Kp_head*e_theta # Set desired angular velocity proportional to the heading error

    # Velocity PID-controller loop
    v     = data.qvel[0]  # Longitudinal velocity of the mobile base
    omega = data.qvel[5]  # Angular velocity of the mobile base 
    e_vel   = Vel_lin_des - v     # Compute the velocity linear error
    e_omega = Vel_ang_des - omega # Computer the angular velocity error
    
    # Use the desired linear and angular velocities as manipulated variables
    # implementing PID controllers for the velocities
    Accel_lin_des = Accel_lin_des + Kp_vel*(e_vel-e_vel1) \
                        + Ki_vel*e_vel*Ts \
                        + Kd_vel*(e_vel-2*e_vel1+e_vel2)/Ts
    Accel_ang_des = Accel_ang_des + Kp_omega*(e_omega-e_omega1) \
                        + Ki_omega*e_omega*Ts \
                        + Kd_omega*(e_omega-2*e_omega1+e_omega2)/Ts
    e_vel2 = e_vel1
    e_vel1 = e_vel
    e_omega2 = e_omega1
    e_omega1 = e_omega
    
    # Convert desired linear/angular accelerations to right/left wheel torques
    Torque_R = (m*r/2)*Accel_lin_des + (J*r/W)*Accel_ang_des + (c*r/2)*v + (b*r/W)*omega
    Torque_L = (m*r/2)*Accel_lin_des - (J*r/W)*Accel_ang_des + (c*r/2)*v - (b*r/W)*omega

    if (e_dist < L/100): # Turn off wheel torques if robot is close to the target 
        Torque_R = 0
        Torque_L = 0 
        
    print('Torque_R, Torque_L: ', np.round(Torque_R, 4), np.round(Torque_L,4))
    
    # Set actuators
    data.actuator("w_rr").ctrl = Torque_R
    data.actuator("w_rc").ctrl = Torque_R
    data.actuator("w_rf").ctrl = Torque_R
    data.actuator("w_lr").ctrl = Torque_L
    data.actuator("w_lc").ctrl = Torque_L
    data.actuator("w_lf").ctrl = Torque_L


def arm_controller(model, data):

    # Names of the arm joints in the model file
    joint_names_1 = ["fr3_joint1","fr3_joint2","fr3_joint3","fr3_joint4","fr3_joint5","fr3_joint6","fr3_joint7"]
    joint_names_2 = ["fr3_joint1_2","fr3_joint2_2","fr3_joint3_2","fr3_joint4_2","fr3_joint5_2","fr3_joint6_2","fr3_joint7_2"]
    
    ## ID numbers associated to the joints of ARM 1:
    dof_ids_1 = np.array([model.joint(name).id for name in joint_names_1])
    #print('ARM 1 joint-IDs: ', dof_ids_1)
    ## ID numbers associated to the joints of ARM 2:
    dof_ids_2 = np.array([model.joint(name).id for name in joint_names_2])    
    #print('ARM 2 joint-IDs: ', dof_ids_2)
    for i in range(7):  # Loop over each joint
        # Note q_pos_arm has offest 6 because the mobile base's
        # q_pos is stored in indeces [0,1,2,3,4,5,6] <-> x, y, z, q1, q2, q3, q4,
        # where q1, q2, q3 and q4 are the orientation quaternion terms.
        # While q_vel_arm has offset 5 because the mobile base's
        # q_vel is stored in indeces [0,1,2,3,4,5] <-> vx, vy, vz, p, q, r,
        # where p, q, r, are the angular velocities about the base frame axes.
        
        # ARM 1:
        q_pos_arm_1 = data.qpos[dof_ids_1[i] + 6]  # Current joint position
        q_vel_arm_1 = data.qvel[dof_ids_1[i] + 5]  # Current joint velocity
        error_1 = q_ref_arm_1[i] - q_pos_arm_1     # Joint angular error
        tau_1 = Kp_arm*error_1 - Kd_arm*q_vel_arm_1 # Joint torque using PD controller 
        frc_bias_1 = data.joint(f"fr3_joint{i + 1}").qfrc_bias  # Gravity bias force to the joint
        data.actuator(f"fr3_joint{i + 1}").ctrl = frc_bias_1 + tau_1  # Apply control torque 
        
        # ARM 2:
        q_pos_arm_2 = data.qpos[dof_ids_2[i] + 6]  # Current joint position
        q_vel_arm_2 = data.qvel[dof_ids_2[i] + 5]  # Current joint velocity
        error_2 = q_ref_arm_2[i] - q_pos_arm_2     # Joint angular error
        tau_2 = Kp_arm*error_2 - Kd_arm*q_vel_arm_2 # Joint torque using PD controller
        frc_bias_2 = data.joint(f"fr3_joint{i + 1}_2").qfrc_bias  # Gravity bias force to the joint
        data.actuator(f"fr3_joint{i + 1}_2").ctrl = frc_bias_2 + tau_2 # Apply control torque


def controller(model, data):
    # Run the mobile base controller
    base_controller(model, data)
    
    # Run the arms controller
    arm_controller(model, data)
    

# --- Assing the motion controller handler ---
mujoco.set_mjcb_control(controller)

# --- Definition of GFLW callback handler functions ---
mouse_button_left   = False
mouse_button_middle = False
mouse_button_right  = False
mouse_lastx = 0
mouse_lasty = 0
def mouse_button(window, button, act, mods):
    global mouse_button_left, mouse_button_middle, mouse_button_right
    
    mouse_button_left   = (glfw.get_mouse_button(window, glfw.MOUSE_BUTTON_LEFT  ) == glfw.PRESS)
    mouse_button_middle = (glfw.get_mouse_button(window, glfw.MOUSE_BUTTON_MIDDLE) == glfw.PRESS)
    mouse_button_right  = (glfw.get_mouse_button(window, glfw.MOUSE_BUTTON_RIGHT ) == glfw.PRESS)

    # Update mouse position
    if mouse_button_left or mouse_button_middle or mouse_button_right:
        print('Mouse button pressed at:  ', glfw.get_cursor_pos(window) )
    else:
        print('Mouse button released at: ', glfw.get_cursor_pos(window) )        

def mouse_scroll(window, xoffset, yoffset):
    action = mujoco.mjtMouse.mjMOUSE_ZOOM
    mujoco.mjv_moveCamera(model, action, 0.0, -0.05*yoffset, scene, cam)

def mouse_move(window, xpos, ypos):
    global mouse_lastx, mouse_lasty
    global mouse_button_left, mouse_button_middle, mouse_button_right
    
    # Compute mouse displacement, save
    dx = xpos - mouse_lastx
    dy = ypos - mouse_lasty
    mouse_lastx = xpos
    mouse_lasty = ypos

    # No buttons down: nothing to do
    if (not mouse_button_left) and (not mouse_button_middle) and (not mouse_button_right):
        #print('Mouse moved without a button pressed')
        return

    # Get current window size
    width, height = glfw.get_window_size(window)

    # Get shift key state
    PRESS_LEFT_SHIFT  = glfw.get_key(window, glfw.KEY_LEFT_SHIFT)  == glfw.PRESS
    PRESS_RIGHT_SHIFT = glfw.get_key(window, glfw.KEY_RIGHT_SHIFT) == glfw.PRESS
    mod_shift = (PRESS_LEFT_SHIFT or PRESS_RIGHT_SHIFT)

    # Determine action based on mouse button
    if mouse_button_right:
        if mod_shift:
            action = mujoco.mjtMouse.mjMOUSE_MOVE_H
        else:
            action = mujoco.mjtMouse.mjMOUSE_MOVE_V
    elif mouse_button_left:
        if mod_shift:
            action = mujoco.mjtMouse.mjMOUSE_ROTATE_H
        else:
            action = mujoco.mjtMouse.mjMOUSE_ROTATE_V
    else:
        action = mujoco.mjtMouse.mjMOUSE_ZOOM

    mujoco.mjv_moveCamera(model, action, dx/height, dy/height, scene, cam)

# --- Initialize the OpenGL graphics engine using GLFW functions ---
def glfw_init():
    width, height = 1280, 720
    window_name = 'DAMM Simulation' # Dual-arm mobile manipulator simulation
    if not glfw.init():
        print("Could not initialize OpenGL context")
        exit(1)
    
    # Create a windowed mode window and its OpenGL context
    window = glfw.create_window( int(width), int(height), window_name, None, None)
        
    if not window:
        glfw.terminate()
        print("Could not initialize GLFW window")
        exit(1)
    
    glfw.make_context_current(window)
    glfw.swap_interval(1) # Request (activate) v-sync
                          # https://discourse.glfw.org/t/newbie-questions-trying-to-understand-glfwswapinterval/1287
 
    glfw.set_cursor_pos_callback(window, mouse_move)
    glfw.set_mouse_button_callback(window, mouse_button)
    glfw.set_scroll_callback(window, mouse_scroll)
    return window

def mat2euler(mat):
    # c_theta^2*(s_phi^2+c_phi^2)
    # 
    c_theta = np.sqrt(mat[2, 2] * mat[2, 2] + mat[1, 2] * mat[1,2])
    condition = c_theta > np.finfo(np.float64).eps*4.0
    
    euler = np.empty(3)
    
    # psi = -atan((c_theta * s_psi)/(c_theta * c_psi))
    # psi = -atan((s_theta * c_psi * s_phi - s_psi * c_phi)/
    #             (s_theta * s_psi * s_phi + c_psi * c_phi))
    euler[2] = np.where(condition,
                             -np.arctan2(mat[0, 1], mat[0, 0]),
                             -np.arctan2(-mat[1, 0], mat[1, 1]))
    # theta = - atan( s_theta, c_theta)
    euler[1] = -np.arctan2(-mat[0, 2], c_theta)
    # phi = - atan(c_theta * s_phi/ c_theta * c_phi)
    euler[0] = np.where(condition,
                             -np.arctan2(mat[1, 2], mat[2, 2]),
                             0.0)
    return euler

def quat2euler(quat):
    Rmat = np.zeros((9,1))
    mujoco.mju_quat2Mat(Rmat, quat)
    euler = mat2euler(Rmat.reshape(3,3))
    return euler


def main():
    window = glfw_init()
    context = mujoco.MjrContext(model, mujoco.mjtFontScale.mjFONTSCALE_150.value)

    while not glfw.window_should_close(window):
        glfw.poll_events()
        
        # Update simulation in one step
        mujoco.mj_step(model, data)
        #print("qpos: ", data.qpos[:])
        
        # Obtain the size of the framebuffer's viewport
        viewport_width, viewport_height = glfw.get_framebuffer_size(window)
        viewport = mujoco.MjrRect(0, 0, viewport_width, viewport_height)

        # Update the scene
        mujoco.mjv_updateScene(model, data, opt, None, cam,
                               mujoco.mjtCatBit.mjCAT_ALL.value, scene)
         # Add target marker
        scene.ngeom += 1
        mujoco.mjv_initGeom(
              scene.geoms[scene.ngeom-1],
              type=mujoco.mjtGeom.mjGEOM_SPHERE,
              size=[0.125, 0, 0],
              pos= np.array([q_ref_base[0], q_ref_base[1], 0.125]),
              mat=np.eye(3).flatten(),
              rgba=np.array([1, 1, 0, 0.5])
          )
        
        
        # Add a marker on robot base

        scene.ngeom += 1

        mujoco.mjv_initGeom(

              scene.geoms[scene.ngeom-1],

              type=mujoco.mjtGeom.mjGEOM_SPHERE,

              size=[0.7125, 0, 0],

              pos= np.array([data.qpos[0], data.qpos[1], data.qpos[2]]),

              mat=np.eye(3).flatten(),

              rgba=np.array([1, 1, 0, 0.5])

          )   
          
        # Render the scene
        mujoco.mjr_render(viewport, scene, context)

        # Swap OpenGL buffers
        glfw.swap_buffers(window)
 
    # Close GLFW and free visualization storage
    glfw.terminate()
 
if __name__ == '__main__':
    main()
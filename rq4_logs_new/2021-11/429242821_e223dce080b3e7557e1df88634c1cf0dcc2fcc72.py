#!/usr/bin/env python3
import rospy
from geometry_msgs.msg import Twist
from nav_msgs.msg import Odometry
from math import sqrt, atan2, exp, atan, cos, sin, acos, pi, asin, atan2
from tf.transformations import euler_from_quaternion, quaternion_from_euler
from time import sleep
from visualization_msgs.msg import Marker, MarkerArray
from sensor_msgs.msg import LaserScan
import tf
import sys
import numpy as np

global Usat
Usat = 5.0
# Frequencia de simulacao no stage
global freq
freq = 100  # Hz

# Estados do robo
global x_n, y_n, theta_n
x_n = 0.0  # posicao x atual do robo
y_n = 0.0  # posicao y atual do robo
theta_n = 0.0  # orientacao atual do robo

# Estados do robo
global x_goal, y_goal
x_goal = 0
y_goal = 0

# Relativo ao feedback linearization
global d
d = 0.80

# Ganho atrativo e repulsivo
global Kp
Kp = 1

global v_fw, w_z
v_fw = 0.0
w_z = 0.0

global goToGoal
goToGoal = False

# Rotina callback para a obtencao da pose do robo
def callback_pose(data):
    global x_n, y_n, theta_n
    x_n = data.pose.pose.position.x  # posicao 'x' do robo no mundo
    y_n = data.pose.pose.position.y  # posicao 'y' do robo no mundo
    x_q = data.pose.pose.orientation.x
    y_q = data.pose.pose.orientation.y
    z_q = data.pose.pose.orientation.z
    w_q = data.pose.pose.orientation.w
    euler = euler_from_quaternion([x_q, y_q, z_q, w_q])
    theta_n = euler[2]  # orientaco 'theta' do robo no mundo
    return

def calcDistance(x_n, y_n, x_d, y_d):
    return sqrt(((x_d - x_n)**2 + (y_d - y_n)**2))


def callback_laser(data):
    global v_fw, w_z, x_goal, y_goal, x_n, y_n
    laserData = []
    angle_increment = np.around(data.angle_increment,4)
    angle_min = data.angle_min
    for index in range(len(data.ranges)):
        rho = data.ranges[index]
        theta = index*angle_increment + angle_min
        x = rho*np.cos(theta) + x_n
        y = rho*np.sin(theta) + y_n
        d_followed = calcDistance(x,y,x_goal,y_goal)
        laserData.append((rho, theta, x, y, d_followed))
        print(x,y)
    heuristic(laserData)
    #print(v_fw, w_z)

def heuristic(laserData):
    global Kp, Usat, v_fw, w_z, x_goal, y_goal, goToGoal
    minDist = laserData[0][0] + laserData[0][4]
    minIdx = 0
    for i in range(len(laserData)):
        dist = laserData[i][0] + laserData[i][4]
        if(laserData[i][2] < x_goal + 0.2 and laserData[i][2] > x_goal - 0.2 and laserData[i][3] < y_goal + 0.2 and laserData[i][3] > y_goal - 0.2):
            goToGoal = True
        #print('i:', i, 'dist: ',dist)
        if(dist < minDist):
            minDist = dist
            minIdx = i

    print('Idx:',minIdx, ' laser XY: ', laserData[minIdx][2], laserData[minIdx][3])
    if(goToGoal):
        print('goToGoal')
        [x_ref, y_ref, Vx_ref, Vy_ref] = refference_trajectory(x_goal, y_goal)
    else:
        print('goToLaser')
        [x_ref, y_ref, Vx_ref, Vy_ref] = refference_trajectory(laserData[minIdx][2], laserData[minIdx][3])
    [Ux, Uy] = trajectory_controller(x_ref, y_ref, Vx_ref, Vy_ref, Kp, Usat)
    [v_fw, w_z] = feedback_linearization(Ux, Uy)



def refference_trajectory(x_goal, y_goal):
    x_ref = x_goal
    y_ref = y_goal
    Vx_ref = 0
    Vy_ref = 0
    return (x_ref, y_ref, Vx_ref, Vy_ref)

# Rotina para a geracao da entrada de controle
def trajectory_controller(x_ref, y_ref, Vx_ref, Vy_ref, Kp, Usat):
    global x_n, y_n, theta_n
    Ux = Vx_ref + Kp * (x_ref - x_n)
    Uy = Vy_ref + Kp * (y_ref - y_n)
    absU = sqrt(Ux ** 2 + Uy ** 2)
    if (absU > Usat):
        Ux = Usat * Ux / absU
        Uy = Usat * Uy / absU
    return (Ux, Uy)


# Rotina feedback linearization
def feedback_linearization(Ux, Uy):
    global x_n, y_n, theta_n
    global d
    VX = cos(theta_n) * Ux + sin(theta_n) * Uy
    WZ = (-sin(theta_n) / d) * Ux + (cos(theta_n) / d) * Uy
    return (VX, WZ)

# Rotina primaria
def example():
    global x_n, y_n, theta_n, x_goal, y_goal
    rospy.init_node("Q1_node") #inicializa o no "este no"
    pub_stage = rospy.Publisher("/cmd_vel", Twist, queue_size=1)  #declaracao do topico para comando de velocidade
    rospy.Subscriber("/base_pose_ground_truth", Odometry, callback_pose) #declaracao do topico onde sera lido o estado do robo
    rospy.Subscriber("/base_scan", LaserScan, callback_laser)

    #Define uma variavel que controlar a a frequencia de execucao deste no
    rate = rospy.Rate(freq)
    vel = Twist()
    sleep(0.2)

    #a = input('Digite os semieixos (a)')
    #a = float(a)
    
    i = 0
    a = 3 
    # O programa do no consiste no codigo dentro deste while
    while not rospy.is_shutdown(): #"Enquanto o programa nao ser assassinado"
        # Incrementa o tempo
        
        i = i + 1

        vel.linear.x = v_fw
        vel.angular.z = w_z
        pub_stage.publish(vel)

        #Espera por um tempo de forma a manter a frequencia desejada
        rate.sleep()

# Funcao inicial
if __name__ == '__main__':
    try:
        x_goal, y_goal = input('(x_goal, y_goal)').split()
        x_goal, y_goal = [float(i) for i in [x_goal, y_goal]]
        example()
    except rospy.ROSInterruptException:
        pass
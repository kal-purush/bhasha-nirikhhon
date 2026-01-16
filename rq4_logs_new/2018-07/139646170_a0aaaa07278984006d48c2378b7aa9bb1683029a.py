#!/usr/bin/env python  
import rospy
import numpy as np
from nav_msgs.msg import Odometry
from geometry_msgs.msg import Twist
from sensor_msgs.msg import LaserScan

X = 0
Y = 0
A = 2
MAXERR = 0.01
MAXDST = 5.0
MAXANG = 90
MAXVEL = 2.0
G0 = 0
G45 = 44
G90 = 89
G135 = 134
G180 = 179
INF = 999999
TMP = 30
MINDST = 1.5


class Control():
    def __init__(self):
        self.ps = np.zeros(3)
        self.vel = np.zeros(3)
        self.vel_rec = np.zeros(3)
        self.laser = np.zeros(180)
        self.estado = 0
        self.contador = 0
        self.da = 0.0
        self.fa = 0.0
        self.sinal1 = 0
        self.obstaculo = 0
        self.minpoint = INF

        self.subPose = rospy.Subscriber('odom', Odometry, self.odomCallback)
        self.subSpeed = rospy.Subscriber('cmd_vel', Twist, self.speedCallback)
        self.laserScan = rospy.Subscriber('base_scan', LaserScan, self.scanCallback)
        self.pubSpeed = rospy.Publisher('cmd_vel', Twist, queue_size=10)
    
    def solvePose(self, initPos, distPerc):
        return distPerc + initPos
    
    def solveAngle(self, initAng, inputAngle):
        return ((inputAngle+initAng)*180)/np.pi

    def scanCallback(self, msg):
        count = 0
        for element in msg.ranges:
            self.laser[count] = element
            count = count + 1

    def odomCallback(self, msg):
        self.ps[X] = msg.pose.pose.position.x
        self.ps[Y] = msg.pose.pose.position.y
        self.ps[A] = 2 * np.arccos(msg.pose.pose.orientation.w) * np.sign(msg.pose.pose.orientation.z)

    def speedCallback(self, msg):
        self.vel_rec[X] = msg.linear.x
        self.vel_rec[Y] = msg.linear.y
    
    def verArea(self, angle):
        for element in range(84, 94):
            if(angle[element] < MINDST):
                return 1
        return 0

    def setObjective(self, xr, yr):
        dx = xr - self.ps[X]
        dy = yr - self.ps[Y]

        ca = np.sqrt(dx*dy)
        co = np.sqrt(dy*dx)
        hy = np.sqrt((co*co)+(ca*ca))

        # defines the obstacle point more approximate
        if(hy < self.minpoint):
            mx = self.ps[X]
            my = self.ps[Y]
            self.minpoint = hy
        
        dy = abs(dy)
        if dy > MAXERR:
            if dy > MAXDST:
                dy = MAXDST
            fy = (dy/2.0)/MAXDST
        else:
            fy = 0

        dx = abs(dx)
        if dx > MAXERR:
            if dx > MAXDST:
                dx = MAXDST
            fx = (dx/2.0)/MAXDST
        else:
            fx = 0
        
        self.vel[X] = 2.0*np.sqrt(fx*fx+fy*fy)
        sin = co/hy
        ar = np.arcsin(sin) * 180/ np.pi

        if(xr < self.ps[X]):
            ar = 180 - ar
        if(yr < self.ps[Y]):
            ar = -ar
        self.da = ar - self.ps[A]

        if(abs(self.ps[A]) > 90 and abs(ar) > 90):
            if((ar > 0 and self.ps[A] < 0) or (ar < 0 and self.ps[A] > 0)):
                self.da = -self.da

        if self.estado == 0 and self.contador > 2:

            if(abs(self.da) > MAXERR):
                if(abs(self.da) > MAXANG and self.da > 0):
                    self.da = MAXANG
                if(abs(self.da) > MAXANG and self.da < 0):
                    self.da = -MAXANG
                self.fa = self.da/MAXANG
                self.vel[A] = MAXVEL*self.fa
            
            if self.verArea(self.laser) == 1 and self.contador > 2:
                self.estado = 1
        
        elif self.estado == 1 and self.contador > 2:
            if(self.laser[89+(int(self.da))] >= 4):
                self.estado = 0

            if(self.laser[G0] < 2):
                if(self.laser[G45] < 1.5):
                    self.vel[A] = MAXVEL
                elif self.laser[G45] > 1.5 and self.laser[G90] > 3.0:
                    self.vel[A] = MAXVEL * (-0.2)
                else:
                    self.vel[A] = 0
            elif self.laser[G180] < 2:
                if(self.laser[G135] < 1.5):
                    self.vel[A] = -MAXVEL
                elif(self.laser[G135] > 1.5 and self.laser[G90] > 3.0):
                    self.vel[A] = MAXVEL * 0.2
                else:
                    self.vel[A] = 0
            
            if(self.laser[G90] < 1.5):
                if(self.laser[G135] > self.laser[G45]):
                    self.vel[A] = MAXVEL
                else:
                    self.vel[A] = -MAXVEL
                self.vel[X] = self.vel[X]*0.2

        if(self.contador <= 3):
            self.contador = self.contador + 1
    
def main():
    rospy.init_node('bug_tan')


    xr = 20.0
    yr = 30.0
    rate = rospy.Rate(10) # 10 Hz
    c = Control()
    vel = Twist()

    while not rospy.is_shutdown():
        c.setObjective(xr, yr)
        vel.angular.z = c.vel[A]
        vel.linear.x = c.vel[X]
        c.pubSpeed.publish(vel)

        rate.sleep()


if __name__ == "__main__":
    main()
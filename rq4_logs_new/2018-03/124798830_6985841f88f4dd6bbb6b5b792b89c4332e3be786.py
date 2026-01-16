# -*- coding: utf-8 -*-
# @Author: juliangaal
# @Date:   2018-02-13 17:31:11
# @Last Modified by:   juliangaal
# @Last Modified time: 2018-02-24 09:25:00
# Kalman 2d prototype 
import sys
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.mlab as mlab
from mpl_toolkits.mplot3d import Axes3D
import math

I = G = P = x = R = I = Q = A = H = B = []
posX = []
posY = []
posZ = []
gain = []
u = 0.0

def init():
    global I, G, P, x, R, I, Q, A, H, u, B

    sigma = 100

    # initial state vector
    x = np.matrix([[0.0, 0.0, 1.0, 10.0, 0.0, 0.0, 0.0, 0.0, -9.81]]).T

    # initial covariance matrix P
    P = np.eye(9) * sigma

    # Dynamikmatrix A
    dt = 0.01
    A = np.matrix([[1.0, 0.0, 0.0,  dt, 0.0, 0.0, 1/2*dt**2, 0.0, 0.0],
                   [0.0, 1.0, 0.0, 0.0,  dt, 0.0, 0.0, 1/2*dt**2, 0.0],
                   [0.0, 0.0, 1.0, 0.0, 0.0,  dt, 0.0, 0.0, 1/2*dt**2],
                   [0.0, 0.0, 0.0, 1.0, 0.0, 0.0,  dt, 0.0, 0.0],
                   [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0,  dt, 0.0],
                   [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0,  dt],
                   [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
                   [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0],
                   [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0]])

    # print(A*x)
    B = np.matrix([[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]]).T
    u = 0.0;

    # Processkovarianzmatrix Q
    sv = 0.1
    G  = np.matrix([[0.5*dt**2, 0.5*dt**2, 0.5*dt**2, dt, dt, dt, 1.0, 1.0, 1.0]]).T
    Q = G * G.T * sv**2

    H = np.matrix([[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                   [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                   [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]])

    # Measurement Noise Covariance
    ra = 1.0**2
    R = np.diag([ra, ra, ra])

    # Identity Matrix
    I = np.eye(9)

def generateData():
    Hz = 100.0 # Frequency of Vision System
    dt = 1.0/Hz
    T = 1.0 # s measuremnt time
    m = int(T/dt) # number of measurements

    px= 0.0 # x Position Start
    py= 0.0 # y Position Start
    pz= 1.0 # z Position Start

    vx = 10.0 # m/s Velocity at the beginning
    vy = 0.0 # m/s Velocity
    vz = 0.0 # m/s Velocity

    c = 0.1 # Drag Resistance Coefficient
    d = 0.9 # Damping

    Xr=[]
    Yr=[]
    Zr=[]
    for i in range(int(m)):
        accx = -c*vx**2  # Drag Resistance
        
        vx += accx*dt
        px += vx*dt

        accz = -9.806 + c*vz**2 # Gravitation + Drag
        vz += accz*dt
        pz += vz*dt
        
        if pz<0.01:
            vz=-vz*d
            pz+=0.02
        if vx<0.1:
            accx=0.0
            accz=0.0
            
        Xr.append(px)
        Yr.append(py)
        Zr.append(pz)

    sp= 0.1 # Sigma for position noise

    Xm = Xr + sp * (np.random.randn(m))
    Ym = Yr + sp * (np.random.randn(m))
    Zm = Zr + sp * (np.random.randn(m))

    measurements = np.vstack((Xm, Ym, Zm));

    return measurements

def save(x):
    global posX, posY, posZ
    x = np.ravel(x)
    posX.append(float(x[0]))
    posY.append(float(x[1]))
    posZ.append(float(x[2]))

def filter(measurements):
    global I, G, P, x, R, I, Q, A, H, B, u

    hitPlate = False
    for n in range(len(measurements[0])):

        if x[2]<0.01 and not hitPlate:
            x[5] *= -1
            hitPlate = True
        else:
            hitPlate = False
        ### Projection ###
        # Project new state
        x = A * x + B * u
        # Project new Covariance
        P = A * P * A.T + Q

        ### Update ###
        # Check with which Covariance we should continue calculation
        S = H * P * H.T + R
        K = P * H.T * np.linalg.pinv(S)

        Z = measurements[:,n].reshape(H.shape[0],1)
        x = x + K * (Z - H * x)
        P = (I - K * H) * P
        # save state
        save(x)

def plot(measurements, result=False, file={'name': 'Kalman6d', 'save': False}):
    global posX, posY, posZ
    Xm = measurements[0]
    Ym = measurements[1]
    Zm = measurements[2]

    if posX and result:
        fig = plt.figure(figsize=(16,9))
        ax = fig.add_subplot(111, projection='3d')
        ax = fig.gca(projection='3d')
        ax.plot(posX, posY, posZ, label='Kalman Filter Estimate', c='red')
        ax.scatter(Xm, Ym, Zm, label='Measurements', c='black')
        ax.set_xlabel('X')
        ax.set_ylabel('Y')
        ax.set_zlabel('Z')
        ax.legend()
        plt.title('Ball Trajectory estimated with Kalman Filter')       
        
    else:
        # Print data
        fig = plt.figure(figsize=(16,9))
        ax = fig.add_subplot(111, projection='3d')
        ax.scatter(Xm, Ym, Zm, c='gray')

        ax.set_xlabel('X')
        ax.set_ylabel('Y')
        ax.set_zlabel('Z')

        plt.title('Ball Trajectory observed from Computer Vision System (with Noise)')

        #ax.w_xaxis.set_pane_color((1.0, 1.0, 1.0, 1.0))

    # Axis equal§
    max_range = np.array([Xm.max()-Xm.min(), Ym.max()-Ym.min(), Zm.max()-Zm.min()]).max() / 3.0
    mean_x = Xm.mean()
    mean_y = Ym.mean()
    mean_z = Zm.mean()
    ax.set_xlim(mean_x - max_range, mean_x + max_range)
    ax.set_ylim(mean_y - max_range, mean_y + max_range)
    ax.set_zlim(mean_z - max_range, mean_z + max_range)
    



    if file['save']:
        plt.savefig('Kalman6d.png', dpi=300, bbox_inches='tight')

    plt.show()


def plot2d(measurements):
    global posX, posZ

    Xm = measurements[0]
    Zm = measurements[2]

    fig = plt.figure(figsize=(16,9))

    plt.plot(posX, posZ, label='Kalman Filter Estimate')
    plt.scatter(Xm, Zm, label='Measurement', c='gray', s=30)
    # plt.plot(Xm, Zm, label='Real')
    plt.title('Estimate of Ball Trajectory (Elements from State Vector $x$)')
    plt.legend(loc='best',prop={'size':22})
    plt.axhline(0, color='k')
    plt.axis('equal')
    plt.xlabel('X ($m$)')
    plt.ylabel('Y ($m$)')
    plt.ylim(0, 2);
    plt.show()

def plotGain():
    plt.plot(range(len(gain)), gain, label='gain')
    plt.show()

init()
measurements = generateData()
plot(measurements)
filter(measurements)
plot(measurements, result=True, file={'name': 'Kalman6d', 'save': True})
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
import math

I = G = P = x = R = I = Q = A = H = []
posX = []
posY = []
gain = []

def init():
    global I, G, P, x, R, I, Q, A, H

    posX = posY = speedX = speedY = 0
    sigmaPosX = sigmaPosY = sigmaSpeedX = sigmaSpeedY = 1000

    # initial state vector
    x = np.matrix([[posX, posY, speedX, speedY]]).T

    # initial covariance matrix P
    P = np.diag([sigmaPosX, sigmaPosY, sigmaSpeedX, sigmaSpeedY])

    # Dynamikmatrix A
    dt = 0.1
    A = np.matrix([[1, 0, dt, 0],
                   [0, 1, 0, dt],
                   [0, 0, 1, 0 ],
                   [0, 0, 0, 1 ]])

    # print(A*x)

    # Processkovarianzmatrix Q
    sv = 8.8
    G  = np.matrix([[0.5*dt**2],
                    [0.5*dt**2],
                    [dt],
                    [dt]])
    Q = G * G.T * sv**2

    H = np.matrix([[0, 0, 1, 0],
                   [0, 0, 0, 1]])

    # Measurement Noise Covariance
    ra = 10.0**2
    R = np.diag([ra, ra])

    # Identity Matrix
    I = np.eye(4)

def generateData():
    global R
    # Generate data
    m = 200 # Measurements
    vx= 20 # in X
    vy= 10 # in Y

    mx = np.array(vx+np.random.randn(m))
    my = np.array(vy+np.random.randn(m))

    measurements = np.vstack((mx,my))

    # print('Standard Deviation of Acceleration Measurements=%.2f' % np.std(mx))
    # print('You assumed %.2f in R.' % R[0,0])
    # print(measurements.shape)
    return measurements

def save(x, ):
    global posX, posY
    posX.append(np.ravel(x[0]))
    posY.append(np.ravel(x[1]))

def filter(measurements):
    global I, G, P, x, R, I, Q, A, H
    # for n in range(len(measurements[0])):
    # print("M: ", measurements[n])
    ### Projection ###
    # Project new state
    x = A * x 
    # Project new Covariance
    P = A * P * A.T + Q

    ### Update ###
    # Check with which Covariance we should continue calculation
    S = H * P * H.T + R
    K = P * H.T * np.linalg.pinv(S)
    print(S)
    print(np.linalg.pinv(S))
    print(K)

    Z = measurements[:,0].reshape(2,1)
    x = x + K * (Z - H * x)
    P = (I - K * H) * P

    # save state
    save(x)

def plot(measurements):
        # Print data
    fig = plt.figure(figsize=(16,5))
    print(measurements.shape)
    plt.step(range(len(measurements[0])),measurements[0,], label='$\dot x$')
    plt.step(range(len(measurements[1])),measurements[1,], label='$\dot y$')
    plt.ylabel(r'Velocity $m/s$')
    plt.title('Measurements')
    plt.legend(loc='best',prop={'size':18})


    plt.show()

def plotResults():
    plt.scatter(posX, posY, s=20, label='State', c='k')
    plt.scatter(posX[0],posY[0], s=100, label='Start', c='g')
    plt.scatter(posX[-1],posY[-1], s=100, label='Goal', c='r')

    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('Position')
    plt.legend(loc='best')
    plt.axis('equal')
    plt.show()

def plotGain():
    plt.plot(range(len(gain)), gain, label='gain')
    plt.show()

init()
measurements = generateData()
filter(measurements)
# plot(measurements)
# plotResults()
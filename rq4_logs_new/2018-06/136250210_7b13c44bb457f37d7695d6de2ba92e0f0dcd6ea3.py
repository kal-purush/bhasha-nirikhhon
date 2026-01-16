import numpy as np
import myFunctions as mf
import robot_calc_functions as tf
from math import pi
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from numpy import sin, cos, exp

##### Link 1 #####
m1 = 10
L1 = .01
Izz1 = 0.
Ixx1 = m1*L1**2 /12
Iyy1 = m1*L1**2 / 12

I1 = np.array([ [Ixx1, 0., 0.],
                [0., Iyy1, 0.],
                [0., 0., Izz1] ])

M01 = np.array([ [1., 0., 0., 0.],
                 [0., 1., 0., 0.],
                 [0., 0., 1., L1/2],
                 [0., 0., 0., 1.] ])

##### Link 2 #####
m2 = 2.
L2 = 0.618
Izz2 = 0
Ixx2 = 0.
Iyy2 = 0
I2 = np.array([ [Ixx2, 0., 0.],
                [0., Iyy2, 0.],
                [0., 0., Izz2] ])

M12 = np.array([ [1., 0., 0., L2],
                 [0., 1., 0., 0.],
                 [0., 0., 1., L1/2],
                 [0., 0., 0., 1.] ])


##### Link 3 #####
m3 = 4.
L3 = 1.
Izz3 = 0
Ixx3 = 0.
Iyy3 = 0
I3 = np.array([ [Ixx3, 0., 0.],
                [0., Iyy3, 0.],
                [0., 0., Izz3] ])

M23 = np.array([ [1., 0., 0., L3],
                 [0., 1., 0., 0.],
                 [0., 0., 1., 0.],
                 [0., 0., 0., 1.] ])

##### End Effector Link #####
M34 = np.array([ [1., 0., 0., 0.],
                 [0., 1., 0., 0.],
                 [0., 0., 1., 0.],
                 [0., 0., 0., 1.] ])

Mlist = np.array((M01, M12, M23, M34))

G1 = np.append(I1, np.zeros((3,3)), 1)
bottom1 = np.append( np.zeros((3,3)), m1*np.eye(3), 1 )
G1 = np.append(G1, bottom1, 0)

G2 = np.append(I2, np.zeros((3,3)), 1)
bottom2 = np.append( np.zeros((3,3)), m2*np.eye(3), 1 )
G2 = np.append(G2, bottom2, 0)

G3 = np.append(I3, np.zeros((3,3)), 1)
bottom3 = np.append( np.zeros((3,3)), m3*np.eye(3), 1 )
G3 = np.append(G3, bottom3, 0)

Glist = np.array((G1, G2, G3))

Slist = np.array([ [0., 0., 0., 1., 0., 0.],
                   [0., 0., 1., 0., 0., 0.],
                   [0., 0., 1., 0., -L2, 0.],
                   [0., 0., 1., 0., -(L2+L3), 0.],
                   [0, 0, 0., 0., 0., 0] ])

g = [0.,-2,0.]
theta = [-1., pi/2+.2, -.06]
dtheta = [0.,0.,0.]


dt = .0025
N = 4000
numLinks = 3

thetaRecord = np.zeros((numLinks,N))
dthetaRecord = np.zeros((numLinks,N))
tau = np.zeros((numLinks,N))
F_tip = np.zeros((6,N))

thetaRecord[:,0] = theta
dthetaRecord[:,0] = dtheta

taus = mf.inverse_dynamics(theta, dtheta, [0,0], Mlist, Slist, Glist, g, [0]*6)


def control_force(position, velocity):
    p = position[0]
    t1 = (pi/2-position[1])
    t2 = (pi/2-(position[2]+position[1]))
    dp = velocity[0]
    dt1 = -velocity[1]
    dt2 = -(velocity[1]+velocity[2])


    kp = -31.6
    kdp = -98.06
    kt1 = 492.5
    kdt1 = 18.96
    kt2 = -765.
    kdt2 = -352.
    F = kp*p + kdp*dp + kt1*t1 + kdt1*dt1 + kt2*t2 + kdt2*dt2
    
    return F

for i in range(1,N):
    #tau[:,i] = -.125* dthetaRecord[:,i-1]  ## Drag on joints
    tau[0,i-1] = control_force(thetaRecord[:,i-1], dthetaRecord[:,i-1])
    
    ddtheta = mf.forward_dynamics(thetaRecord[:,i-1], dthetaRecord[:,i-1], tau[:,i-1], F_tip[:,i-1], g, Mlist, Glist, Slist)
    
    thetaRecord[:,i] = thetaRecord[:,i-1] + dt*dthetaRecord[:,i-1]

    for j in range(numLinks):
        dthetaRecord[j,i] = dthetaRecord[j,i-1] + dt*ddtheta[j,0]
 
    

theta1 = thetaRecord[0,:]
theta2 = thetaRecord[1,:]
theta3 = thetaRecord[2,:]
u = tau[0,:]

plt.plot(theta1, color='blue')
plt.plot(theta2, color='green')
plt.plot(theta3, color='red')
plt.plot(u, color='black')
plt.show()

#create animation
    
fig = plt.figure()
ax = fig.add_subplot(111, autoscale_on=False, xlim=(-2, 2), ylim=(-2, 2))
ax.grid()

line, = ax.plot([], [], 'o-', lw=2)
time_template = 'time = %.1fs'
time_text = ax.text(0.05, 0.9, '', transform=ax.transAxes)

def init():
    line.set_data([],[])
    time_text.set_text('')
    return line, time_text

def animate(i):
    thisx = [theta1[i], theta1[i]+L2*cos(theta2[i]),
             theta1[i]+L2*cos(theta2[i]), theta1[i]+L2*cos(theta2[i]) + L3*cos(theta2[i]+theta3[i]) ] #,
            ## L1*cos(theta1[i]) + L2*cos(theta1[i]+theta2[i]), L1*cos(theta1[i]) + L2*cos(theta1[i]+theta2[i]) + L3*cos(theta1[i]+theta2[i]+theta3[i])]
    thisy = [0, L2*sin(theta2[i]),
             L2*sin(theta2[i]), L2*sin(theta2[i]) + L3*sin(theta2[i]+theta3[i]) ]#,
           ##  L1*sin(theta1[i]) + L2*sin(theta1[i]+theta2[i]), L1*sin(theta1[i]) + L2*sin(theta1[i]+theta2[i]) + L3*sin(theta1[i]+theta2[i]+theta3[i])]

    line.set_data(thisx, thisy)
    time_text.set_text(time_template % (i*dt))
    return line, time_text

ani = animation.FuncAnimation(fig, animate, np.arange(1, N), interval=dt/2, blit=True, init_func=init)
plt.show()



















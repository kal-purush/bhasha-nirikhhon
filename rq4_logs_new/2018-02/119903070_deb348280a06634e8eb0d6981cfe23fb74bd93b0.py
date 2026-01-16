#   File: BicycleCalc.py
#   Author: Nawaf Abdullah
#   Creation Date: 4/Feb/2018
#   Description: Calculates and plots the trajectory of a cannon shell
import matplotlib.pyplot as plt
from math import cos, sin, radians, sqrt

# Parameters Definitions:
# vo  = initial velocity
# ang = launch angle
# dt  = time steps
# rho = density of air
# A   = frontal surface area
# C   = Drag coefficient
# m   = mass of object
# N   = Number of time steps
#   Earth gravitational constant:
g = 9.807


#   Calculates shell trajectory with no drag
def shell_calc_nodrag(vo, ang, dt, N):
    vx = [vo*cos(radians(ang))]
    vy = [vo*sin(radians(ang))]
    x = [0]
    y = [0]
    t = [0]
    for i in range(N):
        vx.append(vx[i])
        vy.append(vy[i] -g*dt)
        x.append(x[i] + vx[i]*dt)
        y.append(y[i] + vy[i]*dt)
        t.append(t[i] + dt)
        if y[i] < 0:
            break
    cap = "Cannon Shell with no drag"
    return x, y, vx, vy, t, cap


#   Calculates shell trajectory with drag
def shell_calc_drag(vo, ang, dt, rho, A, C, m, N):
    vx = [vo*cos(radians(ang))]
    vy = [vo*sin(radians(ang))]
    x = [0]
    y = [0]
    t = [0]
    for i in range(N):
        #   TODO: figure out what is making the projectile move to the left and fix it
        v = sqrt(vx[i]*vx[i] + vy[i]*vy[i])
        a_D = ((C*rho*A*v*v)/(2*m))
        a_Dx = a_D*cos(radians(ang))
        a_Dy = a_D*sin(radians(ang))
        vx.append(vx[i] - a_Dx*dt)
        vy.append(vy[i] - g*dt - a_Dy*dt)
        x.append(x[i] + vx[i]*dt)
        y.append(y[i] + vy[i]*dt)
        t.append(t[i] + dt)
        if y[i] < 0:
            break
    cap = "Cannon Shell with drag"
    return x, y, vx, vy, t, cap


#   plot x vs y trajectory
def shell_plot(x, y, vx, vy, t, cap):
    fig1 = plt.figure(1)
    fig1.suptitle(cap)
    plt.xlabel("x-trajectory")
    plt.ylabel("y-trajectory")
    plt.plot(x, y)
    plt.show()
#   TODO: add velocity vs time plots
#    fig2 = plt.figure(2)
#    fig2.suptitle("Velocity components vs time")
#    plt.subplot(211)
#    plt.plot(vx, t)
#    plt.subplot(212)
#    plt.plot(vy, t)
#    plt.show()


# main
#   t_x, t_y, t_vx, t_vy, t_t, title = shell_calc_nodrag(vo=700, ang=45, dt=0.1, N=2000)
t_x, t_y, t_vx, t_vy, t_t, title = shell_calc_drag(vo=700, ang=45, dt=0.1, rho=1.229, A=0.3, C=0.5, m=50, N=2000)
shell_plot(t_x, t_y, t_vx, t_vy, t_t, title)

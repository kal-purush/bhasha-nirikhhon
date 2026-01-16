# -*- coding: utf-8 -*-
"""
Created on Sun Nov  7 08:12:24 2021

@author: raggarwal
"""


"""
Looks at the controllability ellipsoids as a function of body angular velocity
and inertia tensor
"""

import numpy as np
import matplotlib.pyplot as plt
from dynamics import dq_dt, dq_dt_moments
import control as control
import control.matlab as cmb


# %% Initialization

plt.close("all");

Ixx = 1.0; # kg-m^2, roll inertia

# Initial angular velocity
wx = 1.0;
wy = 0;
wz = 0;

# Initial state and control effort
q = np.array([wx, wy, wz]);
u = np.array([0, 0, 0]);


nPoints = 50;
Iyy_vals = np.linspace(.01, 10*Ixx, nPoints);
Izz_vals = np.linspace(.01, 10*Ixx, nPoints); 

Iyy_grid, Izz_grid = np.meshgrid(Iyy_vals, Izz_vals, indexing = 'ij');

singularValues_ctrb_matrix = np.zeros((nPoints, nPoints, 3)); # Iyy, Izz, sigma 1/2/3



# %% Locally linearize dynamics

for i in range(nPoints):
    Iyy = Iyy_vals[i];
    
    for j in range(nPoints):
        Izz = Izz_vals[j];
        
        A = np.zeros((3, 3));
        B = np.zeros((3, 3));
        
        delta = .01;
        
        for k in range(3):
            dq = np.zeros((3, ), dtype = 'float64');
            dq[k] = delta;
            
            A[:, k] = np.ravel((dq_dt_moments(q + dq, u, Ixx, Iyy, Izz) - 
                                dq_dt_moments(q - dq, u, Ixx, Iyy, Izz))/(2*delta));
            
        for k in range(3):
            du = np.zeros((3, ), dtype = 'float64');
            du[k] = delta;
            
            B[:, k] = np.ravel((dq_dt_moments(q, u + du, Ixx, Iyy, Izz) - 
                                dq_dt_moments(q, u - du, Ixx, Iyy, Izz))/(2*delta));
            
            
            
        # Construct state space models
        sys_c = control.StateSpace(A, B, np.eye(3), np.zeros((3, 3)));
        sys_d = cmb.c2d(sys_c, Ts = .01, method = 'zoh');
        
        ctrb = cmb.ctrb(sys_d.A, sys_d.B);
        U, S, VT = np.linalg.svd(ctrb);
        
        singularValues_ctrb_matrix[i, j, :] = np.ravel(S);




# %% Plot singular values of controllability matrix

fig1, ax1 = plt.subplots(1, 3, constrained_layout = True);
num_lines = 50; 
cmap_string = "turbo";

sigma_min = np.log(np.min(singularValues_ctrb_matrix));
sigma_max = np.log(np.max(singularValues_ctrb_matrix));

cb0 = ax1[0].contourf(Iyy_grid, Izz_grid, np.log(singularValues_ctrb_matrix[:, :, 0]), levels = np.linspace(sigma_min, sigma_max, num_lines), cmap = cmap_string)
ax1[0].set_xlabel("Iyy [kg-m^2]")
ax1[0].set_ylabel("Izz [kg-m^2]")
ax1[0].set_title("log(σ_1)")
cbar0 = fig1.colorbar(cb0, ax = ax1[0])

cb1 = ax1[1].contourf(Iyy_grid, Izz_grid, np.log(singularValues_ctrb_matrix[:, :, 1]), levels = np.linspace(sigma_min, sigma_max, num_lines), cmap = cmap_string)
ax1[1].set_xlabel("Iyy [kg-m^2]")
ax1[1].set_ylabel("Izz [kg-m^2]")
ax1[1].set_title("log(σ_2)")
cbar1 = fig1.colorbar(cb1, ax = ax1[1])

cb2 = ax1[2].contourf(Iyy_grid, Izz_grid, np.log(singularValues_ctrb_matrix[:, :, 2]), levels = np.linspace(sigma_min, sigma_max, num_lines), cmap = cmap_string)
ax1[2].set_xlabel("Iyy [kg-m^2]")
ax1[2].set_ylabel("Izz [kg-m^2]")
ax1[2].set_title("log(σ_3)")
cbar2 = fig1.colorbar(cb2, ax = ax1[2])
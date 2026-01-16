import matplotlib
import matplotlib.pyplot as plt

import scipy
from scipy import integrate
import numpy as np
import matplotlib.animation as animation

# # parameters
WIRE_RADIUS = 0.6 * 10 ** -3  # WIRE RADIUS (M)
L = 100  # WIRE LENGTH (M)
E = 128 * 10 ** 9  # YOUNGS MODULUS (PA)
RHO = 8960  # DENSITY KG/M3
MU = RHO * np.pi * WIRE_RADIUS ** 2  # LINEAR DENSITY (KG/M)
N = 20  # NUM OF ELEMENTS
DELTAX = L / N  # CONNECTION LENGTH
ELEMENT_MASS = np.ones(N) * MU * DELTAX  # ARRAY OF MASSES OF EACH POINT OF SYSTEM
ELEMENT_MASS[0] = MU * DELTAX  # MASS OF SATELLITE
ELEMENT_MASS[-1] = MU * DELTAX  # MASS OF ENDMASS
DELTA_T = 0.01  # TIMESTEP
K = (E * np.pi * (WIRE_RADIUS ** 2)) / DELTAX  # SPRING CONSTANT OF EACH CONNECTION
C = 3 * N  # DAMPING OF EACH CONNECTION
NUM_TIMESTEPS = 10000 # DURATION OF SIMULATION
TENSIONS = []  # A list of tensions on all points on the tehter at checked times
THETA = 2* np.pi * 15 /360
position_history = []  # particle history
tension_history = []
# -----------------------------------------


# physics equations

def ext_acc(A):
    return [0, -9.81, 0]


# ----------------------------------------


# SIMULATION EQUATIONS
# Fuctions to apply the physics along the length of the tether
def internal_accelerations(x, v, C, ELEMENT_MASS, do_tension_check= False):





    # damp
    AxBx = x[1:N, :] - x[:N-1, :]
    AxBxnorm = np.linalg.norm(AxBx, axis=1)[np.newaxis].T
    AvBv = v[1:N, :] - v[:N-1, :]
    AxBx_hat = AxBx / AxBxnorm

    AvBv_dot_AxBx_hat = (AvBv*AxBx_hat).sum(axis=1)[np.newaxis].T
    vprojection = AvBv_dot_AxBx_hat * AxBx_hat

    FBA = -C * vprojection



    # spring
    indices = np.arange(N-1)[np.newaxis].T
    mask = indices[AxBxnorm - DELTAX > 0]
    F = K * (AxBxnorm - DELTAX)
    FAB = F * AxBx_hat

    if do_tension_check:
        a = np.linalg.norm(FBA + FAB, axis =1)
        return a
        # pass
    Aacceleration = -FBA / ELEMENT_MASS[:N - 1][np.newaxis].T
    Bacceleration = FBA / ELEMENT_MASS[1:N][np.newaxis].T

    Aacceleration_ = FAB / ELEMENT_MASS[:N - 1][np.newaxis].T
    Bacceleration_ = -FAB / ELEMENT_MASS[1:N][np.newaxis].T

    a = np.zeros((N, 3))
    a[:N - 1, :] += Aacceleration
    a[1:N, :] += Bacceleration

    a[:N - 1, :][mask] += Aacceleration_[mask]
    a[1:N, :][mask] += Bacceleration_[mask]
    return a



def external_accelerations(x):  # At the moment this is just gravity for testing purposes
    # Will be Handeled by James' Program
    a = np.ones((N, 3)) * np.array([0, -9.81, 0])
    return a



def diff_eq(t, y):
    """Takes input of Y, a 1D array of all position and velocity components appended,
    and outputs the derivatives of these components"""
    x = y[:3 * N]
    x = x.reshape(N, 3)
    vflat = y[3 * N:]
    for i in range(3):
        vflat[i] = 0

    v = vflat.reshape(N, 3)
    VDot = (internal_accelerations(x, v, C, ELEMENT_MASS) +
            external_accelerations(x))
    derv = np.append(vflat, VDot.reshape(3 * N))

    return [derv]

# ---------------------
def animate_position_history(i):
    positions.set_data(position_history[i][:,0], position_history[i][:,1])
    return positions,
def animate_history(i):
    positions.set_data(position_history[i][:, 0], position_history[i][:, 1])
   # tensions.set_data(np.arange(N-1)*DELTAX, tension_history[i] )
    return positions,# tensions,
# ----------------------------------------
# Initial Conditions

# initialise arrays to store

# energy_history = []
ELEMENT_POSITIONS = np.zeros((N, 3))
ELEMENT_VELOCITIES = np.zeros((N, 3))

# initial conditions
for i in range(N):
    position_vector = [DELTAX * i *np.sin(THETA),-DELTAX * i *np.cos(THETA) , 0]  # where each point sits
    ELEMENT_POSITIONS[i, :] = position_vector  # an array stores the entire position of the system


# plt.plot(ELEMENT_POSITIONS[:, 0], ELEMENT_POSITIONS[:, 1])
# plt.show()

# creating 1 dimensional position and velocity arrays
FLAT_ELEMENT_POSITIONS = ELEMENT_POSITIONS.reshape(3 * N)
FLAT_ELEMENT_VELOCITIES = ELEMENT_VELOCITIES.reshape(3 * N)

for i in range(NUM_TIMESTEPS + 1):
    Y = np.append(FLAT_ELEMENT_POSITIONS, FLAT_ELEMENT_VELOCITIES)
    a_t = (0, DELTA_T)
    asol = scipy.integrate.solve_ivp(diff_eq, a_t, Y, vectorized=True, method='RK45')

    finsol = asol.y[:, -1]
    FLAT_ELEMENT_POSITIONS = finsol[0:(3 * N)]
    ELEMENT_POSITIONS = FLAT_ELEMENT_POSITIONS.reshape(N, 3)
    FLAT_ELEMENT_VELOCITIES = finsol[3 * N:]
    # ELEMENT_VELOCITIES = FLAT_ELEMENT_VELOCITIES.reshape(N, 3)
    if i % 1 == 0:
        position_history = position_history + [ELEMENT_POSITIONS]
        #tension_history = tension_history + [internal_accelerations(ELEMENT_POSITIONS, ELEMENT_VELOCITIES, C ,ELEMENT_MASS, do_tension_check= True )]

    print(i)

    # plt.plot(ELEMENT_POSITIONS[:, 0], ELEMENT_POSITIONS[:, 1])
    # axes = plt.gca()
    # axes.set_ylim([-100, 100])
    # axes.set_xlim([-100, 100])
    # plt.show()

#
#
#
#
fig = plt.figure(figsize=(20, 20))
ax1 = fig.add_subplot(111)
ax1.set_title("Positions")
ax1.set_ylim([-100, 100])
ax1.set_xlim([-100, 100])
positions, = ax1.plot([], [])
animation_object = animation.FuncAnimation(fig, animate_history, frames=len(position_history), interval=10, blit=True)
# ax2 = fig.add_subplot(212)
# ax2.set_title("Tensions")
# ax2.set_ylim([-12.5, 2.5])
# ax2.set_xlim([0, 10])
# tensions, = ax2.plot([], [])
#
plt.show()
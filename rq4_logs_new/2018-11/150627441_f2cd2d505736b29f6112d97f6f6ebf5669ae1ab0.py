# UCSD Phys 239, HW 3
# 111218
# PYMJ

import numpy as np
import matplotlib.pyplot as plt
plt.ion()
import sys
import argparse
from matplotlib.ticker import FuncFormatter
from scipy.fftpack import fft, ifft

# Bohr radius [centimeters]
a0 = 5.29e-9
# q = 1.6e-20 # Coulomb [but need CGS, so, 1.6e-20]
q = 5e-10 # franklin (esu) [cm^3/2 g^1/2 s^-1]
Z = 1
m = 1e-27 # 9.1e-28 g

# Set default initial positions and velocities
nb = (500., 500.) # number of Bohr radii
r0 = (nb[0]*a0, nb[1]*a0) # initial position [cm]
v0 = (1e7, 0) # initial velocity [cm/s]
n = 1000 # number of time steps
dt_def = 1e-13 #s
c = 3e10 #cm/s

def path(nb=nb, v0=v0, n=n, plot=True, newfig=False, ms=2, dt=None, 
        plotspectrum=False, timeplots=False):
    """
    r0:     number of Bohr radii at which the e- approaches the ion
    """

    r0 = (nb[0]*a0, nb[1]*a0)
    x = np.empty((n,)); x[0] = r0[0]
    y = np.empty((n,)); y[0] = r0[1]
    vx = np.empty((n,)); vx[0] = v0[0]
    vy = np.empty((n,)); vy[0] = v0[1]
    ax = np.empty((n,)); ay = np.empty((n,)) 
    fx = np.empty((n,)); fy = np.empty((n,))
    t = np.empty((n,)); t[0] = 0

    if dt is None:
        # dt ~ 2b/v0
        dt = r0[1]/v0[0] 
    print ('dt_calc=%s, dt_default=%s' % (dt, dt_def))
                        
    # f ~ Ze^2/b^2
    # m*dv/dt = Ze^2/b^2
    for i in range(n-1):  
        # use current position to find force b/n e- and p+
        # first find the acceleration
        # ax = m*d(vx)/dt               ay = Z*e^2/m*y^2
        fx[i] = -q*q / (x[i]*x[i]);     fy[i] = -q*q / (y[i]*y[i])
        ax[i] = fx[i] / m;              ay[i] = fy[i] / m
                
        # use current x and v to find next x after dt
        x[i+1] = x[i] + vx[i] * dt;       y[i+1] = y[i] + vy[i] * dt
        vx[i+1] = vx[i] - ax[i] * dt;   vy[i+1] = vy[i] + ay[i] * dt

        t[i+1] = t[i] + dt
        # print (' ', i)
        # print ('  x[i], y[i]', x[i], y[i])
        # print ('   vx[i], vy[i]', vx[i], vy[i])
        # print ('    ax[i], ay[i]', ax[i], ay[i])
        # print ('     fx[i], fy[i]', fx[i], fy[i])

    # Turn x and y back into Bohr radius units
    x = x/float(a0); y = y/a0
    
    # print ('\nTimescale of intxn ~ %s; dt ~ %s)' % (dt, dt))
    
    # clear variables
    ayft=None
    axft=None
    # Spectrum of radiation:
    # dW/dw = 8piw^4/3c^4 * |d(w)|^2
    # FT(acceleration): since dv is mostly in y-dir, we take FT of ay which is 
    # the perpendicular acceleration
    # axft = fft(ax)
    ayft = fft(ay)
    omega = t
    # lamb = 2*np.pi/omega
    
    #The power spectrum
    
    if plotspectrum:
        plt.subplots(2, 1, sharex=True, figsize=(11,7))
        plt.suptitle(r'(a) Position ($x_{0}=%da_{0}$, $y_{0}=%da_{0}$);' 
                    '(b) Velocity ($v_{x0}=%s$ \n$v_{y0}=%d$); '
                    '(c) Acceleration'  % (nb[0], nb[1], '{:.0e}'.format(v0[0]), v0[1]), y=0.97)
        axis = plt.subplot(211)
        axis.plot(omega, ay)
        axis.set_ylabel('$a_{y}$')
        axis = plt.subplot(212)
        axis.plot(omega, ayft)
        axis.set_ylabel('$FT(a_{y})$')
        
    # y0=y[0]; vx0 = vx[0]
    # freq = np.linspace(1e-15, 1e-6, n)
    # W = 8/(3*np.pi) * q^6/(m^2*c^3*y0^2*vx0^2)*freq
    # plt.figure()
    # plt.plot(freq, W, '.')

    if plot:
        if newfig: 
            fig, ax0 = plt.subplots(4, 1, sharex=True, figsize=(11,7))
            plt.suptitle(r'(a) Position; (b) Velocity; (c) Acceleration;  dt=%s' % dt, y=0.97)
        axis = plt.subplot(411)
        axis.plot(x, y, '-', zorder=8)
        axis.plot(x, y, '.', ms=ms, zorder=10, label='$x_{0}=%da_{0}$ \n$y_{0}=%da_{0}$' % (nb[0], nb[1]))
        axis.set_xlabel('x [$a_{0}$]'); axis.set_ylabel('y [$a_{0}$]')
        axis.legend(numpoints=1)

        axis = plt.subplot(412)
        axis.plot(vx, vy, '-', zorder=8)
        axis.plot(vx, vy, '.', ms=ms, zorder=10, label='$v_{x0}=%s$ \n$v_{y0}=%d$' % ('{:.0e}'.format(v0[0]), v0[1]))
        axis.set_xlabel('v $[cm/s]$'); axis.set_ylabel('v $[cm/s]$')
        axis.legend(numpoints=1)

        axis = plt.subplot(413)
        # axis.plot(ax, ay, '-', zorder=8)
        axis.plot(ax, ay, '.', ms=ms, zorder=10)
        axis.set_xlabel('a $[cm/s^{2}]$'); axis.set_ylabel('a $[cm/s^{2}]$')    

        #The power spectrum
        axis = plt.subplot(414)
        axis.plot(t, ayft)#, '.', ms=ms)
        axis.set_ylabel('FT($a_{y}$)')
        axis.set_xlabel('$\omega$')


    if timeplots:
        fig2, ax2 = plt.subplots(3, 2, sharex=True, figsize=(12,8))
        plt.suptitle(r'(a) Position w time; (b) Velocity; (c) Acceleration', y=0.97)
        axis = plt.subplot(321)
        axis.plot(t, x, '.', ms=ms, zorder=10)
        axis.set_xlabel('t [s]'); axis.set_ylabel('x [$a_{0}$]')

        axis = plt.subplot(322)
        axis.plot(t, y, '.', ms=ms, zorder=10)
        axis.set_xlabel('t [s]'); axis.set_ylabel('y [$a_{0}$]')

        axis = plt.subplot(323)
        axis.plot(t, vx, '.', ms=ms, zorder=10)
        axis.set_xlabel('t [s]'); axis.set_ylabel('vx [cm/s]')

        axis = plt.subplot(324)
        axis.plot(t, vy, '.', ms=ms, zorder=10)
        axis.set_xlabel('t [s]'); axis.set_ylabel('vy [cm/s]')

        axis = plt.subplot(325)
        axis.plot(t, ax, '.', ms=ms, zorder=10)
        axis.set_xlabel('t [s]'); axis.set_ylabel('$a_{x}$ $(F_{x})$')

        axis = plt.subplot(326)
        axis.plot(t, ay, '.', ms=ms, zorder=10)
        axis.set_xlabel('t [s]'); axis.set_ylabel('$a_{y}$ $(F_{x})$')


if __name__ == '__main__':
    
    print ('\n\n=============================================================')
    print ('\nPhysics 239, HW 3')
    parser = argparse.ArgumentParser()

    # Option to save the figure
    ftypes = ['png', 'jpg', 'jpeg', 'pdf']
    parser.add_argument('-s', '--savefig', action='store',
                        default=False, choices=ftypes,
                        help='Save figure with one of the following formats: \
                        "{}"'.format('", "'.join(ftypes)) )

    # Optional inputs for Q 2: finding I_nu(D) at one frequency
    parser.add_argument('--sig_nu', action='store', default=3.24e-21,
                        type=float, help='Choose a value for the initial intensity \
                        (float)' )
    parser.add_argument('--I_nu_0', action='store', default=2,
                        type=float, help='Choose a value for the initial intensity \
                        (Any int or float)' )
    parser.add_argument('--S_nu', action='store', default=1,
                        type=float, help='Choose a value for the source function \
                        (Any int or float)' )
    args = parser.parse_args()

    if not args.savefig:
        print ('Figures will be automatically saved in code directory')
        args.savefig=True


# ========== Q.1 - Calculating the column density and cross section ============
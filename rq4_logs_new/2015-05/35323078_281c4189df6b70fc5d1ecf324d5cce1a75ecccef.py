# coding: utf-8
"""Navier solution of a simply supported rectangular plate.

Accelerated using numba in nopython mode.

Author: Juan Luis Cano Rodríguez <juanlu001@gmail.com>

References
----------
* Timoshenko, S., & Woinowsky-Krieger, S. (1959). "Theory of plates and
  shells" (Vol. 2, p. 120). New York: McGraw-hill.
* Efunda.com, 2014, eFunda: Classical Plate Case Study. [online]. 2014.
  [Accessed 24 December 2014]. Available from:
  http://www.efunda.com/formulae/solid_mechanics/plates/casestudy_list.cfm#SSSS

"""
from __future__ import division

import numpy as np
from numpy import pi, sin

# In case JIT optimization is desired
try:
    import numba
    jit = numba.njit
    # raise ImportError  # Force no jitting
except ImportError:
    jit = lambda f: f


@jit
def a_mn_point(P, a, b, xi, eta, mm, nn):
    """Navier series coefficient for concentrated load.

    """
    return 4 * P * sin(mm * pi * xi / a) * sin(nn * pi * eta / b) / (a * b)


@jit
def plate_displacement(xx, yy, ww, a, b, P, xi, eta, D, max_m, max_n):
    max_i, max_j = ww.shape
    for mm in range(1, max_m):
        for nn in range(1, max_n):
            for ii in range(max_i):
                for jj in range(max_j):
                    a_mn = a_mn_point(P, a, b, xi, eta, mm, nn)
                    ww[ii, jj] += (a_mn / (mm**2 / a**2 + nn**2 / b**2)**2
                                   * sin(mm * pi * xx[ii, jj] / a)
                                   * sin(nn * pi * yy[ii, jj] / b)
                                   / (pi**4 * D))


if __name__ == '__main__':
    # --- Initial data

    # Plate geometry
    a = 1.0  # m
    b = 1.0  # m
    h = 50e-3  # m

    # Material properties
    E = 69e9  # Pa
    nu = 0.35

    # Series terms
    max_m = 16
    max_n = 16

    # Computation points
    # NOTE: With an odd number of points the center of the place is included in
    # the grid
    NUM_POINTS = 101

    # Load
    P = 10e3  # N
    xi = a / 2
    eta = a / 2

    # Flexural rigidity
    D = h**3 * E / (12 * (1 - nu**2))

    # ---

    # Set up domain
    x = np.linspace(0, a, num=NUM_POINTS)
    y = np.linspace(0, b, num=NUM_POINTS)
    xx, yy = np.meshgrid(x, y)

    # Compute displacement field
    ww = np.zeros_like(xx)
    plate_displacement(xx, yy, ww, a, b, P, xi, eta, D, max_m, max_n)

    # Print maximum displacement
    w_max = abs(ww).max()
    print "Maximum displacement = %14.12f mm" % (w_max * 1e3)
    print "alpha = %7.5f" % (w_max / (P * a**2 / D))
    print "alpha * P a^2 / D = %6.4f mm" % (0.01160 * P * a**2 / D * 1e3)

    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D
    plt.rc('axes.formatter', limits=(-3, 3))

    fig = plt.figure(figsize=(6, 4))
    ax = fig.add_subplot(111, projection='3d')

    surf = ax.plot_surface(xx, yy, ww, cmap=plt.cm.Spectral)

    plt.savefig("navier_plate.png")
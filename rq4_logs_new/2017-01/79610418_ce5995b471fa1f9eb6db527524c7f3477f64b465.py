#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 01:51:20 2017
@author: pawel
"""

import matplotlib

matplotlib.use('qt5Agg')
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit

plt.rcParams['font.family'] = 'serif'
plt.rcParams['font.serif'] = 'Palatino'
plt.rcParams['text.usetex'] = True
plt.rcParams['text.latex.unicode'] = True
plt.rcParams.update({'font.size': 20})
plt.rcParams['text.latex.preamble'] = r'\usepackage[T1]{polski}'
plt.rcParams['figure.figsize'] = 23, 16


def f(x, a, b):
    return a * x + b


def exp(t, i_0, t_0):
    return i_0 * np.exp(t / t_0)


def get_t_0(a, da):
    t0 = 1 / a
    dt = abs(-1 / a ** 2) * da
    return t0, dt


def get_i_0(b, db):
    i_th = np.e ** b
    di_th = abs(np.e ** b) * db
    return i_th, di_th


temp_635 = np.linspace(5, 35, 7) + 273
I_th_635 = [19.1, 20.7, 22.6, 25.0, 27.9, 31.4, 36]
error_I_th_635 = [0.2, 0.2, 0.2, 0.2, 0.3, 0.5, 2]

ax1 = plt.subplot(131)
ax1.errorbar(temp_635, I_th_635, yerr=error_I_th_635, fmt='o')
ax1.set_xlabel("temperatura, $T$ [K]")
ax1.set_ylabel("prąd progowy, $I_{\mathrm{th}}$ [mA]")
plt.grid(True)

ax2 = plt.subplot(132)
ax2.plot(temp_635, np.log(I_th_635), 'ro')
ax2.set_xlabel("temperatura, $T$ [K]")
ax2.set_ylabel("$\ln(I_{\mathrm{th}})$")
ax2.text(288, 3.06, r"$\ln(I_{\mathrm{th}}) = \frac{T}{T_{0}} + \ln I_{0}$")
ax2.text(288, 3.02, "$T_0 = (47 \pm 2)$\,K")
ax2.text(286, 2.98, "$I_0 = (0.05 \pm 0.02)$\,mA")
plt.grid(True)

popt, pcov = curve_fit(f, temp_635, np.log(I_th_635))
a = popt[0]
b = popt[1]
error = np.abs(np.diag(pcov) ** 0.5)
da = error[0]
db = error[1]

t_0, dt_0 = get_t_0(a, da)
i_0, di_0 = get_i_0(b, db)
print("t_0 = %s +- %s" % (t_0, dt_0))
print("i_0 = %s +- %s" % (i_0, di_0))

x = np.linspace(5, 36, 100) + 273
y = a * x + b
ax2.plot(x, y, 'b--')

ax3 = plt.subplot(133)
ax3.set_xlabel("temperatura, $T$ [K]")
ax3.set_ylabel("prąd progowy, $I_{\mathrm{th}}$ [mA]")
ax3.errorbar(temp_635, I_th_635, yerr=error_I_th_635, fmt='o')
f_exp = exp(x, i_0, t_0)
ax3.text(280, 30, r"$y=0.05 \cdot \exp (\frac{T}{47})$", color='green')
ax3.plot(x, f_exp, 'g-')
plt.grid(True)

plt.subplots_adjust(left=0.08, right=0.95, top=0.93, bottom=0.1, hspace=0.34, wspace=0.320)
plt.show()
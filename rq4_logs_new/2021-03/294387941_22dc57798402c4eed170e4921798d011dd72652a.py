# -*- coding: utf-8 -*-
"""
Created on Sat Mar 20 04:01:48 2021

@author: amit
"""

import numpy as np
import pandas as pd
import astropy.units as u
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import matplotlib.lines as mlines
#import seaborn as sns
from astropy.cosmology import Planck13, z_at_value 
# Planck 13 cosmology parameters and z_at_value function
#from astropy.constants import M_sun # value of M_sun as constant in kg
#Msun = u.def_unit('Msun', M_sun) # Msun as unit
from astropy.constants import G # in unit of m^3 kg^-1 s^-2
G = (G.to(u.Mpc**3 / (u.M_sun * u.Gyr**2))).value 
# in unit of Mpc^3 M_sun^-1 Gyr^-2

## linestyles
linestyles = {
     'solid':                 (0, ()),
     'densely dotted':        (0, (1, 1)),
     'densely dashed':        (0, (5, 1)),
     'densely dashdotted':    (0, (3, 1, 1, 1)),
     'densely dashdotdotted': (0, (3, 1, 1, 1, 1, 1))}

l1 = linestyles['densely dotted']
l2 = linestyles['densely dashed']
l3 = linestyles['densely dashdotted']
l4 = linestyles['densely dashdotdotted']
l5 = linestyles['solid']

## redshift array
z = np.linspace(0.,1.8,19)

##############################################################################
## computing timescales -- 
## t_inf-peri: from 2.5r_vir (inf) to peri 
## t_ff: from r_vir to peri

c_200_coma = 4 # NFW characterization parameter (c) for Coma cluster, 
          # value taken from Ludlow+14
c_vir_coma = c_200_coma/0.73 # c_200 converted to virial converted to virial

coma = pd.read_csv('Coma_props.csv') # reading table having Coma props
r_vir_coma = coma.loc[0,'Rvir'] # Coma virial radius in Mpc
M_vir_coma = coma.loc[0, 'Mvir'] # Coma virial mass in M_sun

def fx(x):
    return (np.log(1+x)) + (x/(1+x))

def comp_tff(r, r_vir, M_vir, c_vir):
    x = r/r_vir
    f_cx = fx(c_vir*x)
    f_c = fx(c_vir)
    M_r = M_vir * (f_cx / f_c)
    rho_r = (3/(4*np.pi)) * (M_r/r**3)
    tff = 0.5 / np.sqrt(G * rho_r)
    return tff

t_ff = comp_tff(r_vir_coma, r_vir_coma, M_vir_coma, c_vir_coma)
t_ff_inf_peri = comp_tff(2.5*r_vir_coma, r_vir_coma, M_vir_coma, c_vir_coma)

# scaling with redshift
t_ff_z = t_ff * (1+z)**(-1.5)
t_ff_inf_peri_z = t_ff_inf_peri * (1+z)**(-1.5)

##############################################################################

##############################################################################
## t_dyn: dynamical timescale
Omz = Planck13.Om(z) 
rhoc_z = Planck13.critical_density(z).to(u.M_sun / u.Mpc**3)
# Delta_vir
x = Omz - 1
Del_vir_z = (18*np.pi**2 + 82*x -39*x**2)/(x+1)
rho_z = Del_vir_z * Omz * rhoc_z

t_dyn_z = (1 / np.sqrt(G * rho_z)).value # in Gyr

##############################################################################

##############################################################################
## computing t_depl: gas depletion timescale scaled with redshift
## taconni 18 expession: t_depl = (1+z)^-0.6 * del_MS^-0.44
## Assuming del_MS ~ 10^0 and 10^-1.3
t_depl_z1 = ((1+z)**(-0.6)) * ((10**(0))**(-0.44)) # del_Ms = 10^0
t_depl_z2 = ((1+z)**(-0.6)) * ((10**-1.3)**(-0.44)) # del_Ms = 10^-1.3

##############################################################################

##############################################################################
## quenching timescales from other studies at different redshifts
# Taranu 14
z_t14 = 0.02
tq_t14 = 4
tqe_t14 = 0.5

# Wetzel 13
z_w13 = 0.02
tq_w13 = 4.4
tqe_w13 = 0.4

# Balogh 16
z_b16 = 1
tq_b16 = 1.5
tqe_b16 = 0.5

# Haines 15
z_h15 = 0.2
tq_h15 = 3.7
tqe_h15 = 0.5

# Muzzin 14
z_m14 = 1
tq_m14 = 1
tqe_m14 = 0.25

# Foltz 18
z1_f18 = 1
tq1_f18 = 1.3
tqe1_f18 = 0.5

z2_f18 = 1.5
tq2_f18 = 1.1
tqe2_f18 = 0.5

##############################################################################

##############################################################################
## extractig infall and pericenter times and converting them in redshift
# reading file and savinf infall and pericenter time arrays
fname = './cumMs_at_inf_peri/miles/Rvir_Ms/Rvir_Ms_miles_cum_Ms_at_exp_orb_time_table.csv'
inf_peri_df = pd.read_csv(fname)
inf = np.array(inf_peri_df['tinf'])
peri = np.array(inf_peri_df['tperi'])

# converting time arrays into redshfit
inf_ages = (Planck13.age(0).value - inf) * u.Gyr
inf_z = np.array([z_at_value(Planck13.age, age) for age in inf_ages])
peri_ages = (Planck13.age(0).value - peri) * u.Gyr
peri_z = np.array([z_at_value(Planck13.age, age) for age in peri_ages])

##############################################################################

##############################################################################
## Plotting
c = 'k'
fig, ax = plt.subplots(figsize=(8,7))
# freefall time from r_vir to peri
ax.plot(z, t_ff_z, c='k', linestyle=l1, linewidth=3) 
# freefall time from 2.5r_vir (inf) to peri
ax.plot(z, t_ff_inf_peri_z, c='k', linestyle=l2, linewidth=3) 
# dynamical timescale
#ax.plot(z, t_dyn_z, c='k', linestyle=l5, linewidth=3) 
# gas depletion timescale for del_Ms = 10^0
ax.plot(z, t_depl_z1, c='k', linestyle=l3, linewidth=3) 
# gas depletion timescale for del_Ms = 10^-1.3
#ax.plot(z, t_depl_z2, c='k', linestyle=l4, linewidth=3) 
## quenching timescales from low and high z studies
# Foltz 18
ax.errorbar(z1_f18, tq1_f18, yerr=tqe1_f18, elinewidth=1, capsize=5, 
            ecolor=c, marker='o', mec=c, mfc=c, markersize=10)
ax.errorbar(z2_f18, tq2_f18, yerr=tqe2_f18, elinewidth=1, capsize=5, 
            ecolor=c, marker='o', mec=c, mfc=c, markersize=10)
# Muzzin 14
ax.errorbar(z_m14, tq_m14, yerr=tqe_m14, elinewidth=1, capsize=5, 
            ecolor=c, marker='D', mec=c, mfc=c, markersize=8)
# Haines 15
ax.errorbar(z_h15, tq_h15, yerr=tqe_h15, elinewidth=1, capsize=5, 
            ecolor=c, marker='s', mec=c, mfc=c, markersize=9)
# Balogh 16
ax.errorbar(z_b16, tq_b16, yerr=tqe_b16, elinewidth=1, capsize=5, 
            ecolor=c, marker='*', mec=c, mfc=c, markersize=10)
# Wetzel 13
ax.errorbar(z_w13, tq_w13, yerr=tqe_w13, elinewidth=1, capsize=5, 
            ecolor=c, marker='P', mec=c, mfc=c, markersize=10)
# Taranu 14
ax.errorbar(z_t14, tq_t14, yerr=tqe_t14, elinewidth=1, capsize=5, 
            ecolor=c, marker='X', mec=c, mfc=c, markersize=10)
# rug plot 
height_val = ax.get_ylim()[1]/18
_, ymax = ax.get_ybound()
for x in inf_z:
    ax.axvline(x, ymax=height_val/ymax, linewidth=2, color='r')
for x in peri_z:
    ax.axvline(x, ymax=height_val/ymax, linewidth=2, color='k')
# plot settings
ax.set_ylim(-0.1,7.1)
ax.set_yticks(ax.get_yticks()[1:-1]) # Remove first and last ticks
ax.set_xlim(-0.02,1.62)
ax.set_xticks(ax.get_xticks()[1:-1]) # Remove first and last ticks
ax.set_xlabel(r'$z$', fontsize=18)
ax.set_ylabel(r'$t \, \mathrm{[Gyr]}$', fontsize=18)
ax.xaxis.set_minor_locator(AutoMinorLocator())
ax.yaxis.set_minor_locator(AutoMinorLocator())
ax.tick_params(axis='both',which='major',direction='in', bottom = True, 
                top = True, left = True, right = True, length = 10, 
                labelsize=18)
ax.tick_params(axis='both',which='minor',direction='in', bottom = True, 
                top = True, left = True, right = True, length = 5, 
                labelsize=18)
#dyn = mlines.Line2D([], [], color=c, marker='None', linestyle=l5, linewidth=2,
#                    label=r'$t_\mathrm{dyn}$')
ffip = mlines.Line2D([], [], color=c, marker='None', linestyle=l2, linewidth=2,
                     label=r'$t_\mathrm{ff}\,$($r=2.5r_\mathrm{vir}$)')
ff = mlines.Line2D([], [], color=c, marker='None', linestyle=l1, linewidth=2,
                     label=r'$t_\mathrm{ff}\,$($r=r_\mathrm{vir}$)')
dep1 = mlines.Line2D([], [], color=c, marker='None', linestyle=l3, linewidth=2,
                      label=r'$t_\mathrm{depl}$')
#dep2 = mlines.Line2D([], [], color=c, marker='None', linestyle=l4, linewidth=2,
#                      label=r'$t_{\mathrm{depl},\, \delta_{\mathrm{MS}}=10^{-1.3}}$')
circ = mlines.Line2D([], [], color=c, marker='o', linestyle='None', 
                     markersize=10, label=r'$t_{\mathrm{q}}\,$(F18)')
diam = mlines.Line2D([], [], color=c, marker='D', linestyle='None', 
                     markersize=8, label=r'$t_{\mathrm{q}}\,$(M14)')
sqar = mlines.Line2D([], [], color=c, marker='s', linestyle='None', 
                     markersize=9, label=r'$t_{\mathrm{q}}\,$(H15)')
star = mlines.Line2D([], [], color=c, marker='*', linestyle='None', 
                     markersize=10, label=r'$t_{\mathrm{q}}\,$(B16)')
plus = mlines.Line2D([], [], color=c, marker='P', linestyle='None', 
                     markersize=10, label=r'$t_{\mathrm{q}}\,$(W13)')
cros = mlines.Line2D([], [], color=c, marker='X', linestyle='None', 
                     markersize=10, label=r'$t_{\mathrm{q}}\,$(T14)')
i = mlines.Line2D([], [], color='r', marker='|', linestyle='None',
                              markersize=15, label=r'$z_\mathrm{inf}$')
p = mlines.Line2D([], [], color='k', marker='|', linestyle='None',
                              markersize=15, label=r'$z_\mathrm{peri}$')
h1 = [plus, cros, diam, i, ff, dep1, sqar, star, circ, p, ffip]
ax.legend(handles=h1, ncol=2, loc=1, fontsize=15, frameon=True, 
                 framealpha=1.0, edgecolor='k', bbox_to_anchor=(0.98,0.98), 
                 bbox_transform=ax.transAxes)
# h2 = [plus, cros, diam, sqar, star, circ]
# leg2 = ax.legend(handles=h2, ncol=2, loc=1, fontsize=15, frameon=True, 
#                  framealpha=1.0, edgecolor='k', bbox_to_anchor=(0.98,0.74), 
#                  bbox_transform=ax.transAxes)
# ax.add_artist(leg1)

plt.savefig('tq.pdf',dpi=500)
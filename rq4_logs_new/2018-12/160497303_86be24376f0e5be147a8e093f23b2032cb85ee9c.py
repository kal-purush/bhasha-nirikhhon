# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 14:37:18 2018

@author: mandreini
"""

import numpy as np
import heli
import engine

#heliinputs = 'HAMRAC', 1, 8, 2657*9.81, 8950, 8.49, 0.457, 3, 650, 0.95, 21.21, 2, 0.012, 1.15

#SR_HAMRAC = heli.SRheli(*heliinputs)
#marilena = heli.SRheli("Marilena's Example", 1, 4, 90e3, 0, 9.5, 0.457, 3, 2000, 0.95, 21, 3, 0.01, 1.15)
#airspeed = np.arange(1, 120, 1)
#mp = marilena.powerCurve(airspeed2)

# Single Rotor Sample Power curve
#level_speed, level_power = SR_HAMRAC.powerCurve(airspeed, P_to0=None, figtitle='Power requirements for a rotorcraft in level, horizontal, forward flight', fname='PowerCurveHAMRACLevel.png')
#level_vchars = HAMRAC.determineV_chars(airspeed, level_power/1000)
#climb_speed, climbing_power = SR_HAMRAC.powerCurve(airspeed, level_power, 'Power requirements for a rotorcraft in non-level, climbing flight', fname='PowerCurveHAMRACClimb.png')
#climb_vchars = HAMRAC.determineV_chars(airspeed, level_power/1000)

# Coaxial Rotor Sample Power Curve
#Coax_HAMRAC = heli.Coaxheli(*heliinputs, zR=0.19, convert=True)
#clvl_v, clvl_p = Coax_HAMRAC.powerCurve(airspeed, P_to0=None, figtitle='Power requirements for a rotorcraft in level, horizontal, forward flight', fname='PowerCurveCoaxLevel.png')
#ccl_v, ccl_p = Coax_HAMRAC.powerCurve(airspeed, clvl_p, 'Power requirements for a rotorcraft in non-level, climbing flight', fname='PowerCurveCoaxClimb.png')

# Rotor size difference between single-rotor and coaxial-rotor configurations
#print('Parameter | single-rotor | coaxial-rotor\nNr |', SR_HAMRAC.Nr, '|', Coax_HAMRAC.Nr, '\nNb |', SR_HAMRAC.Nb, '|', SR_HAMRAC.Nb, '\nR |', SR_HAMRAC.R, '|', Coax_HAMRAC.R, '\nc |', SR_HAMRAC.c, '|', Coax_HAMRAC.c, '\nOmega |', SR_HAMRAC.Omega, '|', Coax_HAMRAC.Omega)

# Parameter optimisation
#Rvs, Rdata = HAMRAC.idealPhovafoR()
#HAMRAC.plot_val(Rdata[0], Rdata[1][0]/1000, title='Optimising power required to hover wrt blade radius', xlabel='Blade radius R [m]', ylabel='P_hov [kW]', get_min=True)
#
#cvs, cdata = HAMRAC.idealPhovafoc()
#HAMRAC.plot_val(cdata[0], cdata[1][0]/1000, title='Optimising power required to hover wrt blade chord', xlabel='Blade chord c [m]', ylabel='P_hov [kW]', get_min=True)
#
#Omegavs, Omegadata = HAMRAC.idealPhovafoOmega()
#HAMRAC.plot_val(Omegadata[0], Omegadata[1][0]/1000, title='Optimising power required to hover wrt angular velocity', xlabel='Angular velocity Omega [rad/s]', ylabel='P_hov [kW]', get_min=True)

# Characteristic airspeed parameter optimisation (2D parameter optimisation)
#v_charvs, v_chardata = HAMRAC.v_charsafoR(airspeed)#np.linspace(1,100,10), Rs=np.arange(5,10,1))
#v_charRs, v_charspeeds = v_chardata
#v_charRs = v_charRs[0]  # 'unmesh'
#v_As, v_Bs, v_Cs, p_mins, p_sfrs = v_charspeeds
#v_As, v_Bs, v_Cs = v_As[::-1], v_Bs[::-1], v_Cs[::-1]
#HAMRAC.plot_val(v_Adata[0][0], v_Avals, xlabel='Radius of rotor R (m)', ylabel='v_pmin; minimum power airspeed', title='Comparison between minimum power airspeed and rotor diameter')

# Preliminary data power curve, Leon, 8.2.4
#m1pavel3 = heli.Coaxheli('m1pavel3', 2, 3, 2657*9.81, 8950, 4.46, 0.304, 3, 650, 0.95, 41.8, 2, 0.012, 1.15)
#m2krenik3 = heli.Coaxheli('m2krenik3', 2, 3, 2657*9.81, 8950, 4.46, 0.304, 3, 650, 0.95, 41.8, 2, 0.012, 1.15)
#m3campfens3 = heli.Coaxheli('m3krenik3', 2, 3, 2657*9.81, 8950, 4.46, 0.293, 3, 650, 0.95, 48.202, 2, 0.012, 1.15)
#
#m1pavel4 = heli.Coaxheli('m1pavel4', 2, 4, 2657*9.81, 8950, 4.46, 0.227, 3, 650, 0.95, 41.8, 2, 0.012, 1.15)
#m2krenik4 = heli.Coaxheli('m2krenik3', 2, 4, 2657*9.81, 8950, 4.46, 0.227, 3, 650, 0.95, 41.8, 2, 0.012, 1.15)
#m3campfens4 = heli.Coaxheli('m3krenik3', 2, 4, 2657*9.81, 8950, 4.46, 0.220, 3, 650, 0.95, 48.202, 2, 0.012, 1.15)
#
#m13_lvl_v, m13_lvl_p = m1pavel3.powerCurve(vs=airspeed, P_to0=None, figtitle='Power requirements for %s in level forward flight' % m1pavel3.name, fname='leonvals/pcurvem13_lvl.png')
#m23_lvl_v, m23_lvl_p = m2krenik3.powerCurve(vs=airspeed, P_to0=None, figtitle='Power requirements for %s in level forward flight' % m2krenik3.name, fname='leonvals/pcurvem23_lvl.png')
#m33_lvl_v, m33_lvl_p = m3campfens3.powerCurve(vs=airspeed, P_to0=None, figtitle='Power requirements for %s in level forward flight' % m3campfens3.name, fname='leonvals/pcurvem33_lvl.png')
#m14_lvl_v, m14_lvl_p = m1pavel4.powerCurve(vs=airspeed, P_to0=None, figtitle='Power requirements for %s in level forward flight' % m1pavel4.name, fname='leonvals/pcurvem14_lvl.png')
#m24_lvl_v, m24_lvl_p = m2krenik4.powerCurve(vs=airspeed, P_to0=None, figtitle='Power requirements for %s in level forward flight' % m2krenik4.name, fname='leonvals/pcurvem24_lvl.png')
#m34_lvl_v, m34_lvl_p = m3campfens4.powerCurve(vs=airspeed, P_to0=None, figtitle='Power requirements for %s in level forward flight' % m3campfens4.name, fname='leonvals/pcurvem34_lvl.png')
#
#m13_cl_v, m13_vl_p = m1pavel3.powerCurve(vs=airspeed, P_to0=m13_lvl_p, figtitle='Power requirements for %s in non-level, climbing flight' % m1pavel3.name, fname='leonvals/pcurvem13_cl.png')
#m23_cl_v, m23_cl_p = m2krenik3.powerCurve(vs=airspeed, P_to0=m23_lvl_p, figtitle='Power requirements for %s in non-level, climbing flight' % m2krenik3.name, fname='leonvals/pcurvem23_cl.png')
#m33_cl_v, m33_cl_p = m3campfens3.powerCurve(vs=airspeed, P_to0=m33_lvl_p, figtitle='Power requirements for %s in non-level, climbing flight' % m3campfens3.name, fname='leonvals/pcurvem33_cl.png')
#m14_cl_v, m14_cl_p = m1pavel4.powerCurve(vs=airspeed, P_to0=m14_lvl_p, figtitle='Power requirements for %s in non-level, climbing flight' % m1pavel4.name, fname='leonvals/pcurvem14_cl.png')
#m24_cl_v, m24_cl_p = m2krenik4.powerCurve(vs=airspeed, P_to0=m24_lvl_p, figtitle='Power requirements for %s in non-level, climbing flight' % m2krenik4.name, fname='leonvals/pcurvem24_cl.png')
#m34_cl_v, m34_cl_p = m3campfens4.powerCurve(vs=airspeed, P_to0=m34_lvl_p, figtitle='Power requirements for %s in non-level, climbing flight' % m3campfens4.name, fname='leonvals/pcurvem34_cl.png')
#
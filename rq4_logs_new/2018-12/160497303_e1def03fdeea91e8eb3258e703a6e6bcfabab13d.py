# -*- coding: utf-8 -*-
"""
Created on Thu Dec  6 09:26:19 2018

@author: mandreini
"""
import numpy as np
import matplotlib.pyplot as plt


class heli(object):
    
    
    def __init__(self, Nr, Nb, W, h, R, c, f, P_e, eta_m, Omega, CDS, CDp_bar, k=1.15):
        '''
        Initialise a helicopter object
        
        Parameters
        ----------
        Nr : int
            Number of rotors
        Nb : int
            Number of blades
        W : int
            Weight of the rotorcraft [N]
        h : float
            Operating altitude of the helicopter
        R : float
            Radius of the blades
        c : float
            Chord length of the blade
        f : float
            Equivalent wetted area of the helicopter. Recommend 0.93 for small helicopter, and 4.65 for large helicopter        
        P_e : float
            Engine power
        eta_m : float
            Mechanical efficiency of the engine, 0<eta_m<1
        Omega : float
            Angular velocity of the rotor in rad/s          
        CDS : float
            Equivalent flat plate area of the helicopter. Values range from 1 (aerodynamicly efficient) to 6 (aerodynamically inefficient)        
        CDp_bar : array_like, float
            (Average) drag coefficient of the rotor blade. Constant when blade is constant        
        '''
        self.Nr = Nr
        self.Nb = Nb
        self.W = W
        self.h = h
        self.R = R
        self.c = c
        self.f = f
        self.P_e = P_e
        self.eta_m = eta_m
        self.Omega = Omega
        self.CDS = CDS
        self.CDp_bar = CDp_bar
        self.k = k
        self.alpha = 0.  # to be altered somehow    
        
    def _calc_params(self):
        '''
        Calculcate design parameters. Allows for them to change as new variables are introduced
        '''
        self.sigma = self.Nb * self.c / (np.pi*self.R)
        self.v_tip = self.Omega*self.R
        self.rho = 1.225*288.16/(15+271.16-1.983*self.h/304.8)*(1-(0.001981*self.h/0.3048)/288.16)**5.256
        self.vi_hov = (np.sqrt(self.W/(2*self.rho*np.pi*self.R**2)))

    def _get_vihov(self, R):
        return np.sqrt(self.W/(2*self.rho*np.pi*R**2))
    
    def plot_val(self, x, y, title=None, xlabel=None, ylabel=None, get_min=False, xlim=None, ylim=None):
        '''
        Plot x against y
        '''
        plt.figure()
        plt.grid()
        
        if not xlim: xlim = (0, max(x))
        if not ylim: ylim = (0, max(y))
        plt.xlim(xlim)    
        plt.ylim(ylim)
        
        if get_min:
            xmin, ymin = self.get_min(x, y)
            plt.axhline(ymin, 0, xmin/xlim[1])
            plt.axvline(xmin, 0, ymin/ylim[1])
        
        if title: plt.title(title)
        if xlabel: plt.xlabel(xlabel)
        if ylabel: plt.ylabel(ylabel)
        plt.plot(x, y)
        plt.show()
        
    def get_min(self, x, y):
        ymin = min(y)
        xmin = x[np.where(y==ymin)][0]
        return xmin, ymin
    
    def setR(self, Rs):
        self.R = Rs

    def setOmega(self, Omegas):
        self.Omega = Omegas

    def setalpha(self, alphas):
        self.alpha = alphas    
    
    def determineP_to(self, v_inf):
        ''' 
        Determine the total power required of a helicopter in level, forward flight
        
        Parameters
        ----------
        v_inf : array_like
            The airspeed(s) of the helicopter in m/s 
            
        Returns
        -------
        P_to : array_like
            The total power required for the helicopter at airspeed
        P_i : array_like
            The induced power for the helicopter at airspeed
        P_pd : array_like
            The profile drag power for the helicopter at airspeed
        P_par : array_like
            The parasite drag power for the helicopter at airspeed
        '''
        
        myu = v_inf/(self.Omega*self.R)
        vi_cr = self.vi_hov**2 / np.sqrt((v_inf*np.cos(self.alpha))**2+(v_inf*np.sin(self.alpha)+self.vi_hov)**2)  # from cunha lecture 6
        P_i = self.W*vi_cr
        P_pd = self.sigma*self.CDp_bar/8 * self.rho * ((self.Omega*self.R)**3) * np.pi*self.R**2 * (1+3*myu**2)  # to be given from Aero department, note: highly sensity to blade radius (R^5)!
        P_par = self.CDS*0.5*self.rho*v_inf**3
        
        P_to = P_i + P_pd + P_par # marilena
        
        return P_to, P_i, P_pd, P_par
    
    def powerCurve(self, vs=np.arange(0, 105, 1)):
        '''
        Generate a power curve for various airspeeds
        '''
        
        return vs, self.determineP_to(v_inf=vs)
            
    def idealPhovafoR(self, Rs=np.linspace(1,11)):
        '''
        This function will generate a graph P_hov(R)
        '''
        self.setR(Rs)
        self._calc_params()
        Pi_hovs = self.determineP_to(v_inf=0)
        
        return Rs, Pi_hovs
        
HAMRAC = heli(1, 8, 2657*9.81, 8975, 8.49, 0.457, 3, 632e3, 0.95, 21.21, 2, 0.012, 1.15)
Rdata = HAMRAC.idealPhovafoR()
#Pcurve = HAMRAC.powerCurve()
#HAMRAC.plot_vals(Pcurve[0], Pcurve[1][0])
HAMRAC.plot_val(Rdata[0], Rdata[1][0]/1000, title='Optimizing power required to hover wrt blade radius', xlabel='Blade radius R [m]', ylabel='P_hov [kW]', get_min=True)
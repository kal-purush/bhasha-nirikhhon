# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 14:28:58 2018

@author: mandreini
"""

class Engine(object):
    
    def __init__(self, P_sl, weight, fuel_flow):
        '''
        Create an engine object
        To be changed as i.e. Max does the engine work, this is a basic structure
        
        Parameters
        ----------
        P_sl : float
            Power available at sea level
        weight : float
            Weight of the engine, in N
        fuel_flow : float
            Fuel flow mass of the engine, in N/s
        '''
        
        self.P_sl = P_sl
        self.lapse = lapse
        self.weight = weight
        self.fuel_flow = fuel_flow
        
    @staticmethod
    def get_rho(h):
        return 1.225*288.16/(15+271.16-1.983*h/304.8)*(1-(0.001981*h/0.3048)/288.16)**5.256
    
    @staticmethod
    def get_T(h):
        return 298.13 + -0.0065*h
        
    def determine_power_at_altitude(self, h):
        '''
        Function to determine power provided by the engine at given altitude
        The math is made up at the moment
        
        Parameters
        ----------
        altitude : array_like
            (An array of) operating altitude(s)
        
        Returns
        -------
        array_like :
            power provided at given altitude(s)
        '''
        rhoa = self.get_rho(h)
        rho0 = self.get_rho(0)
        Ta = self.get_T(h)
        T0 = self.get_T(0)
        
        # 0.1% decrease per rho0/rhoa, 0.05% increase per T0/Ta
        return self.P_sl/(rho0/rhoa*0.001)*(T0/Ta*0.0005)
    
h125 = Engine(632, 250*9.81, 2.3)
h9kPa = H125.determine_power_at_altitude(9000)
print(h9kPa)
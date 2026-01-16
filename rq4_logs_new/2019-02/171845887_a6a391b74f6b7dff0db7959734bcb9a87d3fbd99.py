# -*- coding: utf-8 -*-
"""
Created on Thu Feb 21 10:19:09 2019

@author: Lucie
"""

import roblib 
import numpy as np




class Particule:
    def __init__(self):
        self.x = np.zeros((3,1))
        self.Mco = np.zeros((3,3))
        
    def __repr__(self):
        return "Vecteur etat :[0},{},{}]".format(self.x[0],self.x[1],self.x[2]) + "\n Matrice de covariance : {}".format(self.Mco)  # {:.2f} notation pour n'afficher que deux chiffres après la virgule
        
    def controle(self):
        return 0, 0
    
    def f(x,theta,u):
        """ equation d'etat de l'AUV"""
        x = x.flatten()
        A  = np.array([[0,0,np.cos(theta)],[0,0,np.sin(theta)],[0,0,-1]])
        return A @ x + np.array([[0,0,u]]).T


        
        
       
    
   
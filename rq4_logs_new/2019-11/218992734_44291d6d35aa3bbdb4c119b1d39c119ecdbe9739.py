#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 08:49:04 2019

@author: adrien
"""

from numpy import *

class solarNetwork:
    #contient les poids des neurones
    
    def __init__(self):
        self.network=[10,10,10,10,7]
        self.layer1Input=[0]*self.network[0]#contient dans chaque case les différentes entrées de la couche 1 (la plus complexe)
        self.layer1Weight=[0]*self.network[0]#la couche 1 prend un nombre différents
        self.layer2Input=[0]*self.network[1]
        self.layer2Weight=[0]*self.network[1]
        self.layer3Input=[0]*self.network[2]
        self.layer3Weight=[0]*self.network[2]
        self.layer4Input=[0]*self.network[3]
        self.layer4Weight=[0]*self.network[3]
        self.layer5Input=[0]*self.network[4]
        self.layer5Weight=[0]*self.network[4]
        
 
    def neurone (self,inputN,weight):
        #on prend liste input et liste poids
        outputN=0
        for i in range (len(inputN)):
            outputN+=arctan(inputN[i]*weight[i])#on peut remplacer arctan par autre chose
        return outputN
    
    def reflection(self,inputNetwork):
        for i in range (self.network[0]):#on prend les input qu'on met sur les input decouche 1
            self
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 19:13:33 2018

@author: alexandre
"""

from roblib import *


############################################
class Particule:
    """
    Particule class
    """
    def __init__(self,X,U,cov):
        """
        Constructor

        Parameters
        ----------
        X: state vector
            X[0]:x coordinate
            X[1]:y coordinate
            X[2]:v speed
            
        U: input vector
            U[0]:u speed input
            U[1]:theta heading input
            
        cov: matrix 4*4
            covariance matrix           
        """
        
        self.X = X
        self.U = U
        self.cov = cov



    def __str__(self):
        """
        Allows to print the Particule object
        """
        X = self.X.flatten()
        U = self.U.flatten()
        return 'X: \n x coordinate > {}\n y coordinate > {}\n speed > {}\n\n U: \n speed input > {}\n theta heading input > {}\n\n cov:\n {}'.format(self.X[0], self.X[1], self.X[2], self.U[0], self.U[1], self.cov)
    

    def __repr__(self):
        X = self.X.flatten()
        U = self.U.flatten()
        return "Vecteur etat : [{},{},{}]".format(self.X[0],self.X[1],self.X[2]) + "\n Matrice de covariance : {}".format(self.cov) + "\n Vecteur d'entree : [{},{}]".format(self.U[0],self.U[1])  # {:.2f} notation pour n'afficher que deux chiffres après la virgule

    
    def display(self,col):
        """
        Allows to display the Particule object through matplotlib
        """
        X = self.X.flatten()
        U = self.U.flatten()
        draw_arrow(X[0],X[1],U[1],0.1,col)
        
    def controle(self):
        """
        Control equation of the AUV
        """
        return 0, 0
    
    def f(self):
        """
        State equation of the AUV 
        """
        U = self.U.flatten()
        theta = U[1]
        A  = array([[0,0,cos(theta)],[0,0,sin(theta)],[0,0,-1]])
        return A.dot(self.X) + array([[0],[0],[U[0]]])
        
if __name__ == "__main__":
    
    X = array([[1],[4],[2]])
    U = array([[3],[pi/4]])
    cov = array([[1,0],[0,1]])
    part = Particule(X,U,cov)
    figure()
    part.display("blue")
    part2 = part
    part2.X = part.f()
    part2.display("red")
    print(part)
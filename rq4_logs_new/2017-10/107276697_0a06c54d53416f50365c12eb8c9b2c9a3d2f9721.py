from math import *
from sympy import *
from sympy.abc import x, y
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches

class Ode(object):

    def __init__(self, delta, steps, function, distribution, discrete, y0, x0):
        self.delta = delta
        self.steps = steps
        self.func = function
        self.dist = distribution
        self.discrete = discrete
        self.y0 = y0
        self.x0 = x0

    def euler(self):
        y0 = self.y0
        if self.func!= False:
            f = lambdify(x, self.func)
        x0 = self.x0
        coords = np.array([x0,self.y0])
        for i in range(self.steps):
            if self.func!= False:
                y1 = y0 + self.delta*f(x0)
            if self.dist != False:
                y1 = y0 + self.delta*self.dist.evaluate(x0)
            if np.any(self.discrete) != False:
                y1 = y0 + self.delta*self.discrete[i,1]
            y0 = y1
            x0 = x0 + self.delta
            temp = np.array([x0,y0])
            coords = np.vstack((coords,temp))
        return coords

    def RK4(self):
        y0 = self.y0
        x0 = self.x0
        if self.func!= False:
            f = lambdify(x, self.func)
        coords = np.array([x0,self.y0])
        for i in range(self.steps):
            if self.func!= False:
                k1 = f(x0)
                k2 = f(x0 + self.delta*0.5)
                k3 = k2
                k4 = f(x0 + self.delta)
            if self.dist != False:
                k1 = self.dist.evaluate(x0)
                k2 = self.dist.evaluate(x0 + self.delta*0.5)
                k3 = k2
                k4 = self.dist.evaluate(x0 + self.delta)
            if np.any(self.discrete) != False:
                k1 = self.discrete[i,1]
                dy = self.discrete[i+1,1]-self.discrete[i,1]
                k2 = self.discrete[i,1] + dy/2
                k3 = k2
                k4 = self.discrete[i+1,1]
            y0 = y0 + self.delta*(float(k1)/6.0 + float(k2)/3.0 + float(k3)/3.0 + float(k4)/6.0)
            x0 = x0 + self.delta
            temp = np.array([x0,y0])
            coords = np.vstack((coords,temp))
        return coords
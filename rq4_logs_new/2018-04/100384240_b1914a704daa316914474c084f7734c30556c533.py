#!/usr/bin/env python

from openravepy import *
import IPython
import numpy as np
import time

from openravepy.misc import InitOpenRAVELogging

InitOpenRAVELogging() 

env = Environment()
env.Load('er4u_env.xml')
env.SetViewer('qtcoin')
robot = env.GetRobots()[0]
viewer = env.GetViewer()
manip = robot.GetActiveManipulator()
RaveSetDebugLevel(DebugLevel.Debug)

viewer.SetCamera([[ -7.42264500e-01,   2.81706167e-02,  -6.69514621e-01,
          7.47426270e+02],
       [  6.70076215e-01,   4.07817975e-02,  -7.41171175e-01,
          1.13386023e+03],
       [  6.42476063e-03,  -9.98770875e-01,  -4.91473407e-02, 
          2.45291901e+02],
       [  0.00000000e+00,   0.00000000e+00,   0.00000000e+00,
          1.00000000e+00]])

viewer.SetBkgndColor([0,0,0])

ikmodel = databases.inversekinematics.InverseKinematicsModel(robot,iktype=IkParameterization.Type.TranslationDirection5D)

if not ikmodel.load():
    ikmodel.autogenerate()

IPython.embed()
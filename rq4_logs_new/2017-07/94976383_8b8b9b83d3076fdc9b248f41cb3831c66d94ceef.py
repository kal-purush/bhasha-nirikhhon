# -*- coding: utf-8 -*-
#------------------------------------------------------------------------------
# Name          : perceptron
# Version       : 0.1.0
# Author        : yxf
# Language      : Python 3.6
# Start time    : 2017-07-03 14:50
# End time      :
# Function      : 
# Operation     :
#------------------------------------------------------------------------------

# System Moduls
import os
import sys
import math

# External Moduls
import cv2
import numpy as np
import matplotlib.pyplot as plt

# Custom Moduls


class Perceptron() :
    def __init__(self) :
        self._w = self._b = None

    # lr 梯度下降法中的学习速率
    # epoch 梯度下降法中的迭代上限
    def fit(self , x , y , lr=0.001 , epoch=1000) :
        x = np.array(x , np.float32)
        y = np.array(y , np.float32)
        self._w = np.zeros(x.shape[1])
        self._b = 0
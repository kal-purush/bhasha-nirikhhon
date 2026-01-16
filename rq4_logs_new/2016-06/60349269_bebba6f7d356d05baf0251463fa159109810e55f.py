#!/usr/bin/python
import sys
import os
import numpy as np
import random
import matplotlib.pyplot as plt

file_in1 = sys.argv[1]
file_in2 = sys.argv[2]
title    = sys.argv[3]
fig_name = sys.argv[4]

data    = np.genfromtxt(file_in1)
measure = data[:,1:]
mean    = np.mean(measure[0, :])
x = data[:,0]
for i in range(5):
    measure[i, :]    = mean/(measure[i, :]*x[i])
mean    = np.mean(measure, axis=1)
std     = np.std(measure, axis=1) 

y = mean
# example error bar values that vary with x-position
plt.axis([1, 17, 0.1, 1.5])
plt.xlabel('# Cores')
plt.ylabel('Scaling Efficiency')
plt.title(title)
plt.errorbar(x,y, yerr=std, label='laki')


data    = np.genfromtxt(file_in2)
measure = data[:,1:]
mean    = np.mean(measure[0, :])
x = data[:,0]
for i in range(5):
    measure[i, :]    = mean/(measure[i, :]*x[i])
mean    = np.mean(measure, axis=1)
std     = np.std(measure, axis=1) 

y = mean
# example error bar values that vary with x-position
plt.errorbar(x,y, yerr=std, label='ulisse')
plt.legend()
plt.savefig(fig_name, format='png')
#plt.show()

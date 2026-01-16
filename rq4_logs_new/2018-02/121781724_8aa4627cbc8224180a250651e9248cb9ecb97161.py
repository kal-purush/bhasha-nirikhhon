import numpy as np
import matplotlib.pyplot as plt
# %matplotlib inline
from imp import reload
import sys, os
import scipy.special as sp
import dill
sys.path.append('../../')
import mg_si
import csv
import datetime
import warnings

from mg_si import plot as mplt

print(sys.argv)

with open('data.m', 'rb') as file:
    pl,times,solution = dill.load(file)
file.close()

all_parameters = pl.core_layer.compute_all_parameters(times, solution)
mplt.Q_all(pl, times, all_parameters)
mplt.E_all(pl, times, all_parameters)
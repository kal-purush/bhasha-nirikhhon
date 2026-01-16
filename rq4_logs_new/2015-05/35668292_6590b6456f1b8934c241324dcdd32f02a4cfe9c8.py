# File:           bayesian_estimation.py
# Version:        1
# Last changed:   Saturday 16th May 2015
# Purpose:        Load model and run Bayesian 
#                 estimation of RFT
# Author:         Dr James Tripp
# Copyright:      (C) James Tripp 2015

# load model definition file
import model_definition

# Needed for MCMC function
from pymc import *

# Initialise model
model = MCMC(model_definition)

# Take sample from model
model.sample(iter=3000)

# Print posterior estimates
print(model.stats())
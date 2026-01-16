from __future__ import division
import numpy as np
import math
from funwalk.proportions import p_yeast,p_mold,p_intermediate

def rmsd(Diff,t_):
    return np.sqrt(6*Diff*t_)

def rmsd_yeast_state(T,fric,t_,T_0,Kb=1.38064852*10**-23):
    Diff = ((Kb*(T))/fric)*p_yeast(T,T_0)
    p_yeast(37,T_0)
    return np.sqrt(6*Diff*t_)

def rmsd_mold_state(T,fric,t_,T_0,Kb=1.38064852*10**-23):
    Diff = ((Kb*(T))/fric)*p_mold(T,T_0)
    p_yeast(37,T_0)
    return np.sqrt(6*Diff*t_)

def rmsd_intermediate_state(T,fric,t_,T_0,Kb=1.38064852*10**-23):
    Diff = ((Kb*(T))/fric)*p_intermediate(T,T_0)
    p_yeast(37,T_0)
    return np.sqrt(6*Diff*t_)
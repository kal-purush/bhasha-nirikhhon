"""
Test behavtask_tr module the following functions:
    merge_cond
    events2neural_extend

Run with::
    **Run from project-theta directory with 'make test'

"""
# Loading modules.
from __future__ import absolute_import, division, print_function
import numpy as np
import os
import sys
from numpy.testing import assert_allclose


# Set path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../functions')))

# Path to the first subject, first run, this is used as the test data for 
pathtotest = 'code/utils/tests/' 

# Load graph_functions:
from behavtask_tr import merge_cond, events2neural_extend

def test_mergecond():
    # my function
    my_merge = merge_cond(pathtotest+'test_behavdata.txt', pathtotest+'test_cond001.txt', pathtotest+'test_cond002.txt', pathtotest+'test_cond003.txt', pathtotest+'test_cond004.txt')
    t_behav = np.loadtxt(pathtotest+'test_behavdata.txt', skiprows=1)
    t_con1 = np.loadtxt(pathtotest+'test_cond001.txt')
    t_con2 = np.loadtxt(pathtotest+'test_cond002.txt')    
    t_con3 = np.loadtxt(pathtotest+'test_cond003.txt')
    t_con4 = np.loadtxt(pathtotest+'test_cond004.txt')
    
    # assert
    assert_allclose(t_behav[:,1:], my_merge[:,6:])
    assert_allclose(t_con1, my_merge[:,:3])
    assert_allclose(t_con2[:,-1], my_merge[:,3])
    assert_allclose(t_con3[:,-1], my_merge[:,4])
    assert_allclose(t_con4[:,-1], my_merge[:,5])
    
def test_events2neuarl_extend():
    t_behav = np.array([np.arange(12)+1, np.arange(12)+2])
    tr = 2
    n_tr = 2
    lame = np.array([1])
    try:
        events2neural_extend(lame, tr, n_tr)
    except ValueError:
        assert(True)
    t_time = np.array([np.arange(3,13), np.arange(4,14)])
    my_time = events2neural_extend(t_behav, tr, n_tr)
    assert_allclose(t_time, my_time)
    
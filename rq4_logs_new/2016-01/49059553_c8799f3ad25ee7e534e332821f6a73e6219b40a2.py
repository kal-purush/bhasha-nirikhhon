#!/usr/bin/python

from math import *
from pylab import *

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D


def get_matrix(n=200, alpha=0.5, radius=1.0): 
    '''
    Build a random nxn matrix of weights.

    n (int):         Number of variables
    alpha (float):   proportion of infinitesimal rotation
    radius (float):  desired spectral radius of the matrix
    '''
    # build a matrix based on alpha
    W = randn(n,n)

    # decompose
    W1 = (W - W.T)/2.   # rotation
    W2 = (W + W.T)/2.   # expansion/rotation

    # recompose
    W = alpha*W1 + (1-alpha)*W2     

    # scale so that the spectral radius is 'radius'
    W = radius*W*(1/max(abs(eigvals(W))))
    
    return W
    
def run_dynamics(W, m=1000, n =200,  h=0.1) :
    ''' 
    Run the dynamics of the system and compute PCA on 
    the time-series.
    
    dx = -x + W*z
    z = tanh(x)
    
    W (nxn matrix):  Matrix of weights of the system
    m (int):         number of timesteps
    n (int):         Number of independent variables

    return:
    X (Mxn):         time series of the n variables 
    T (Mx3):         time series of on the first 3 principal components
    '''

    # dynamics
    x = randn(n)   # initial values
    h = 0.1    # integration step

    X = zeros([m,n])    # data storage

    # integrate over time
    for t in range(m) :
        x += h*(-x + tanh(dot(W,x)))
        X[t,:] = x

    #PCA
    B = X - X.mean(0)   # subtract mean
    C = dot(B.T,B)/float(n-1)    # covariance matrix
    E, W = eig( dot(C.T,C) )     # eigenvalues, eigenvectors
    E = real(E)    # throw off 0j imag part
    W = real(W[argsort(E)[::-1]])    # sort eigenvector (throw of 0j imag part)
    W = W[:,:3]    # get first 3 eigenvectors
    T = dot(B,W)   # first 3 principal components
    
    return T,X

def plot_matrix(W, T) :
    '''
    Plot the spectrogram of the weight matrix
    and the trajectory of the first 3 principal components


    W (nxn matrix):  Matrix of weights of the system
    T (Mx3):         time series of on the first 3 principal components
    '''
    fig = plt.figure(figsize=(8,4))
    
    # plot the spectrogram of the W matrix 
    ax = fig.add_subplot(121)
    EM, _ = eig( W )     
    ax.scatter(real(EM),imag(EM))
    xlim([-(radius*6./4.),(radius*6./4.)])
    ylim([-(radius*6./4.),(radius*6./4.)])


    # plot the trajectory made by the first 3 principal components
    ax = fig.add_subplot(122, projection='3d')
    ax.plot(T[:,0],T[:,1],T[:,2])
    ax.scatter(T[0,0],T[0,1],T[0,2],c="red")
    ax.scatter(T[-1::,0],T[-1::,1],T[-1::,2],c="blue")
    
    fig.canvas.draw()


############################################################################


if __name__ == "__main__" :

    ion()
    close('all')

    n = 200    # number of units 
    radius = 6    # final spectral radius

    # iterate alpha parameter - balance betweeen rot and exp/contr  
    for alpha in (0.2, 0.5, 0.8) :

        # build the weight matrix
        W = get_matrix(n, alpha, radius)
        # run the dynamics
        X,T = run_dynamics(W = W, n = n) 
        # plot 
        plot_matrix(W, T)

    raw_input()
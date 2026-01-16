# -*- coding: utf-8 -*-
"""
Created on Wed Oct 10 10:27:01 2018

@author: WZQ
"""
from numpy import zeros,dot
class HMM:
    
    def __init__(self,A,B,pi):
        self.A =A
        self.B =B
        self.pi =pi
        
    def _forward(self,obs_seq):
        # 取A = N x N
        N = self.A.shape[0]
        T = len(obs_seq)

        F = zeros((N,T))

        # alpha = pi*b
        F[:,0] = self.pi *self.B[:,obs_seq[0]]

        for t in range(1,T):
            for n in range(N):
                # 计算第t时，第n个状态的前向概率
                F[n,t] = dot(F[:,t-1], (self.A[:,n])) * self.B[n, obs_seq[t]]
        return F
    def _backward(self,obs_seq):
        N = self.A.shape[0]
        T = len(obs_seq)

        X = zeros((N,T))
        # 表示X矩阵的最后一列
        X[:,-1:] =1

        for t in reversed(range(T-1)):
            for n in range(N):
                # 边权值为a_ji
                X[n,t] = sum(X[:,t+1] * self.A[n,:] * self.B[:,obs_seq[t+1]])

        return X
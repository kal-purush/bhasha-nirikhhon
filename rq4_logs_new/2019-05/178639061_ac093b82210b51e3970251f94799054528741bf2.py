#!/usr/bin/env python
# coding: utf-8

# # Translation of Bessel function from FORTRAN to Python

# In[5]:


import math
import scipy
import numpy as np
from scipy import special


# Importing is done, and the general function is below.

# In[2]:


def besselj(n, x):
    IACC = 40
    BIGNO = 1e10
    BIGNI = 1e-10
    
    if(n == 0):
        return bessj0(x)
    
    if(n == 1):
        return bessj1(x)
    
    if(x == 0):
        return 0
    
    tox = 2./x
    
    if(x > n):
        bjm = bessj0(x)
        bj = bessj1(x)
        for i in range(1,N-1):
            bjp = i * tox * bj - bjm
            bjm = bj
            bj = bjp
        bessj = bj
    else:
        m = n + math.sqrt(IACC * n)
        
        bessj = 0
        jsum = 0
        bsum = 0
        bjp = 0
        bj = 1
        
        for i in range(m, 1, -1):
            bjm = i * tox * bj - bjp
            bjp = bj
            bj = bjm
            
            if(abs(bj) > BIGNO):
                bj = bj * BIGNI
                bjp = bjp * BIGNI
                bessj = bessj * BIGNI
                bsum = bsum * BIGNI
            
            if(jsum != 0):
                bsum = bsum + bj
            
            jsum = 1 - jsum
            
            if(i == n):
                bessj = bjp
                
        bsum = 2 * bsum - bj
        bessj = bessj / bsum
    
    return bessj


# In[3]:


def bessj0(x):
    P = [1e0,-.1098628627e-2,.2734510407e-4, -.2073370639e-5,.2093887211e-6]
    Q = [-.1562499995e-1,.1430488765e-3, -.6911147651e-5,.7621095161e-6,-.9349451520e-7]
    R = [57568490574e0,-13362590354e0, 651619640.7e0,-11214424.18e0,77392.33017e0,-184.9052456e0]
    S = [57568490411e0,1029532985e0, 9494680.718e0,59272.64853e0,267.8532712e0,1e0]
    
    if(x == 0):
        return 1e0
        
    ax = abs(x)
    
    if(ax < 8):
        y = x ** 2
        
        FR = R[0] + y * (R[1] + y * (R[2] + y * (R[3] + y * (R[4] + y * R[5]))))
        FS = S[0] + y * (S[1] + y * (S[2] + y * (S[3] + y * (S[4] + y * S[5]))))
        
        bessj0 = FR / FS
        
    else:
        z = 8 / ax
        y = z ** 2
        xx = ax - .785398164
        
        FP = P[0] + y * (P[1] + y * (P[2] + y * (P[3] + y * P[4])))
        FQ = Q[0] + y * (Q[1] + y * (Q[2] + y * (Q[3] + y * Q[4])))
        
        bessj0 = math.sqrt(.636619772 / ax) * (FP * math.cos(xx) - z * FQ * math.sin(xx))
        
    return bessj0
    

def bessj1(x):
    P = [1e0,.183105e-2,-.3516396496e-4,.2457520174e-5,-.240337019e-6, .636619772e0]
    Q = [.04687499995e0,-.2002690873e-3, .8449199096e-5,-.88228987e-6,.105787412e-6]
    R = [72362614232e0,-7895059235e0, 242396853.1e0,-2972611.439e0,15704.48260e0,-30.16036606e0]
    S = [144725228442e0,2300535178e0, 18583304.74e0,99447.43394e0,376.9991397e0,1e0]
    
    ax = abs(x)
    
    if(ax < 8):
        y = x ** 2
        
        FR = R[0] + y * (R[1] + y * (R[2] + y * (R[3] + y * (R[4] + y * R[5]))))
        FS = S[0] + y * (S[1] + y * (S[2] + y * (S[3] + y * (S[4] + y * S[5]))))
        
        bessj1 = x * FR / FS
        
    else:
        z = 8 / ax
        y = z ** 2
        xx = ax - 2.35619491
        
        FP = P[0] + y * (P[1] + y * (P[2] + y * (P[3] + y * P[4])))
        FQ = Q[0] + y * (Q[1] + y * (Q[2] + y * (Q[3] + y * Q[4])))
        
        bessj1 = math.sqrt(P[5] / ax) * (math.cos(xx) * FP - z * math.sin(xx) * FQ) * abs(S[5]) * (x / ax)
        
    return bessj1


# In[7]:


test = besselj(1, 4)
official = scipy.special.jv(1,4)

print(test)
print(official)


# In[ ]:




import numpy as np
import scipy as sp
from scipy import stats

def mean_confidence_interval(data, confidence=0.95):
    a = 1.0*np.array(data)
    n = len(a)
    m, se = np.mean(a), stats.sem(a)
    h = se * stats.t._ppf((1+confidence)/2., n-1)
    return stats.sem(a), m, m-h, m+h

print mean_confidence_interval(np.array([1,2,3]))
# print np.std(np.array([1,2,3]),ddof = 1) 
a = np.array([1,2,3])
mean = np.mean(a)
sigma = np.std(a,ddof = 1)
print sigma
print stats.norm.interval( 0.95, loc = mean, scale = sigma/ np.sqrt(3))
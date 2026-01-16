import numpy as np
from numpy.fft import rfft, irfft
#below is your module. Use your ListTimeSeries or ArrayTimeSeries..
import timeseries.TimeSeries as ts
from scipy.stats import norm
from scipy import signal


def tsmaker(m, s, j):
    t = np.arange(0.0, 1.0, 0.01)
    v = norm.pdf(t, m, s) + j*np.random.randn(100)
    return ts.TimeSeries(list(v), list(t))

def random_ts(a):
    t = np.arange(0.0, 1.0, 0.01)
    v = a*np.random.random(100)
    return ts.TimeSeries(list(v), list(t))

def stand(x, m, s):
    "standardize timeseries x my mean m and std deviation s"
    return ts.TimeSeries((x.values() - m) / s, x.times())

def standardize(x):
    return stand(x, x.mean(), x.std())

def ccor(ts1, ts2):
    "given two standardized time series, compute their cross-correlation using FFT"
    # Source code: http://a-ma.us/wp/2011/03/cross-correlation-in-9-lines-of-code/
    x = rfft(ts1)
    y = rfft(ts2[::-1])
    a, b = irfft(rfft(ts1)*rfft(ts1[::-1]))[0], irfft(rfft(ts2)*rfft(ts2[::-1]))[0]
    # www.mathworks.com/help.signal/ref/xcorr.html for normalizing constant
    normConst = len(ts1)
    return irfft(x*y) / normConst #len(ts1)


# this is just for checking the max correlation with the
#kernelized cross-correlation
def max_corr_at_phase(ts1, ts2):
    ccorts = ccor(ts1, ts2)
    idx = np.argmax(ccorts)
    maxcorr = ccorts[idx]
    return idx, maxcorr

#The equation for the kernelized cross correlation is given at
#http://www.cs.tufts.edu/~roni/PUB/ecml09-tskernels.pdf
#normalize the kernel there by np.sqrt(K(x,x)K(y,y)) so that the correlation
#of a time series with itself is 1. We'll set the default multiplier to 1.
def kernel_corr(ts1, ts2, mult=1):
    "compute a kernelized correlation so that we can get a real distance"
    cross_cor = ccor(ts1, ts2)
    K = np.sum(np.exp(mult*cross_cor))
    return K / np.sqrt(np.sum(np.exp(mult*ccor(ts1, ts1))) * np.sum(np.exp(mult*ccor(ts2, ts2))))

def calcDist(ts1, ts2):
    "calculate the distance btw 2 time series"
    return 2 * (1 - kernel_corr(ts1, ts2)) 

#this is for a quick and dirty test of these functions
if __name__ == "__main__":
    t1 = tsmaker(0.5, 0.1, 0.01)
    t2 = tsmaker(0.5, 0.1, 0.01)
    print(t1.mean(), t1.std(), t2.mean(), t2.std())
    import matplotlib.pyplot as plt
    plt.plot(t1)
    plt.plot(t2)
    plt.show()
    standts1 = stand(t1, t1.mean(), t1.std())
    standts2 = stand(t2, t2.mean(), t2.std())

    idx, mcorr = max_corr_at_phase(standts1, standts2)
    print('A',idx, mcorr)
    sumcorr = kernel_corr(standts1, standts2, mult=10)
    print(sumcorr)
    t3 = random_ts(2)
    t4 = random_ts(3)
    plt.plot(t3)
    plt.plot(t4)
    plt.show()
    standts3 = stand(t3, t3.mean(), t3.std())
    standts4 = stand(t4, t4.mean(), t4.std())
    idx, mcorr = max_corr_at_phase(standts3, standts4)
    print(idx, mcorr)
    sumcorr = kernel_corr(standts3, standts4, mult=10)
    print(sumcorr)
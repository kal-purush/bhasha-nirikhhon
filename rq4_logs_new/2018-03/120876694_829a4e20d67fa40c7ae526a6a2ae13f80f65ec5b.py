import matplotlib.pyplot as plt
import numpy as np
import time
from numba import jit

@jit(nopython=True)
def get_bin_width(a, bins):
    a_min = a.min()
    a_max = a.max()
    delta = (a_max - a_min) / bins
    return delta

@jit(nopython=True)
def get_bin_edges(a, bins):
    bin_edges = np.zeros((bins+1,), dtype=np.float64)
    a_min = a.min()
    a_max = a.max()
    delta = (a_max - a_min) / bins
    for i in range(bin_edges.shape[0]):
        bin_edges[i] = a_min + i * delta

    bin_edges[-1] = a_max  # Avoid roundoff error on last point
    return bin_edges

@jit(nopython=True)
def get_bin_midpoints(a, bins):
    bin_edges = get_bin_edges(a, bins)
    a_min = a.min()
    a_max = a.max()
    delta = (a_max - a_min) / bins
    bin_mids = bin_edges + 0.5*delta
    bin_midpoints = bin_mids[:-1]
    return bin_midpoints

@jit(nopython=True)
def compute_bin(x, bin_edges):
    # assuming uniform bins for now
    n = bin_edges.shape[0] - 1
    a_min = bin_edges[0]
    a_max = bin_edges[-1]

    # special case to mirror NumPy behavior for last bin
    if x == a_max:
        return n - 1 # a_max always in last bin

    bin = int(n * (x - a_min) / (a_max - a_min))

    if bin < 0 or bin >= n:
        return None
    else:
        return bin


@jit(nopython=True)
def numba_histogram(a, bins):
    hist = np.zeros((bins,), dtype=np.intp)
    bin_edges = get_bin_edges(a, bins)
    bin_midpoints = get_bin_midpoints(a, bins)

    for x in a.flat:
        bin = compute_bin(x, bin_edges)
        if bin is not None:
            hist[int(bin)] += 1

    return hist, bin_midpoints

@jit(nopython=True)
def tightbind3D(n, t, a):
    
    result = []
    Pi = 3.1415926311

    for i in range(n):
        for j in range(n):
            for k in range(n):
                result.append(-2*t*(np.cos((-Pi/a)+i*(2*Pi)/(n*a)) + np.cos((-Pi/a)+j*(2*Pi)/(n*a)) + np.cos((-Pi/a)+k*(2*Pi)/(n*a))))
    return result

def main():
    
    n = 550 #number of grids (each direction)
    t = 1 #hopping term
    a = 1 #lattice parameter
    bin_number = 350 #number of bins
    
    #Using cpu for regular DOS calculation
    start = time.time()
    res = tightbind3D(n, t, a)
    end_time = time.time()-start
    print("DOS of 3d tight binding model took %s seconds" %end_time)
    res_array = np.array(res) #convert datatype from list to array for numba functions
    delta = get_bin_width(res_array, bin_number) #To calculate probability distribution function
    histogram = numba_histogram(res_array, bin_number) #Use numba_histogram
    plt.plot(histogram[1], histogram[0]/(n*n*n)/delta) #Plot the PDF (not the apparent histogram)
    #plt.hist(res,  bins=325, normed=True) #original slow histogram method
    plt.show()

    #Implement gpu (cuda) jit for faster DOS calculation
    #tightbind3D_fast = jit(double[:,:](double[:,:], double[:,:], double[:,:]))(tightbind3D)
    #start_fast = time.time()
    #res2 = tightbind3D_fast(n, t, a)
    #end_time_fast = time.time()-start_fast
    #print("DOS of 3d tight binding model took %s seconds for gpu." %end_time_fast)
    #plt.hist(res2,  bins=200, normed=True)
    #plt.show()

    #speed=filter2d_time/fastfilter2d_time

    #print("GPU is %s times faster than CPU" %speed)

if __name__ == "__main__":
    main()
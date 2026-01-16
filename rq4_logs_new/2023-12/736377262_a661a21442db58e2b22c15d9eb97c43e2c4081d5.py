import math
import matplotlib.pyplot as plt



def main():


    N = 1000

    damp = 2*10^3
    k = 2*10^7
    m = 50
    kf = 2*10^9
    fn = 119
    zeta = 0.026
    wn = math.sqrt(k/m)

    G, wc = calc_cartesian(400, wn, zeta)

    plt.plot(G, wc)
    plt.show()


    return


def G(wn, wc, zeta):


    transient = (wn^2) - (wc^2)
    res = transient / ((transient^2) + ((2*zeta*wn)^2)*(wc^2))


    return res


def calc_cartesian(wc_range, wn, zeta):

    res = []
    wc_vector = []

    for wc in range(wc_range):
        res.append(G(wn, wc, zeta)) 
        wc_vector.append(wc)
        
    return res, wc_vector



def velocity(T):

    res = 60/T

    return res



def T_vector(wc, n, phi):

    pi = math.pi
    response = []

    for number in range(n):
        transient = (2*number*pi)+(3*pi)+(2*phi)
        transient = transient/wc
        response.append(transient)

    return response
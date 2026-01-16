# coding: utf-8

from __future__ import division, print_function

__author__ = "adrn <adrn@astro.columbia.edu>"

# Standard library
import os
import sys
import urllib2
import warnings

# Third-party
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import truncnorm, scoreatpercentile

# Neutron star distribution properties (fixed)
bounds_NS = (1.3, 2.)  # Msun
mean_NS = 1.4  # Msun
stddev_NS = 0.05  # Msun

# White dwarf mass bounds
bounds_WD = (0.2, 1.44)

# Number of steps to use in numerical integration below
Nintegrate = 4096

def integrand_factor(m2, mf, m1):
    """ Compute the factor multiplying p(M_2|θ) in the integral of Equation XXX in the paper """
    mtot = m1 + m2
    return mtot**(4/3.) * mf**(-1/3.) / m2 / np.sqrt(m2**2 - (mf*mtot**2)**(2/3.)) / 3.

def m2_func(p, mf, m1, bounds_WD, m2s):
    mean_WD,stddev_WD,f_NS = p

    # White dwarf companion mixture component
    lower, upper = bounds_WD
    dist_WD = truncnorm((lower - mean_WD) / stddev_WD, (upper - mean_WD) / stddev_WD, loc=mean_WD, scale=stddev_WD)

    # Neutron star companion mixture component
    lower, upper = bounds_NS
    dist_NS = truncnorm((lower - mean_NS) / stddev_NS, (upper - mean_NS) / stddev_NS, loc=mean_NS, scale=stddev_NS)

    p_WD = (1-f_NS) * dist_WD.pdf(m2s)
    p_NS = f_NS * dist_NS.pdf(m2s)

    return p_WD + p_NS

def likelihood(p, mf, m1, bounds_WD):
    mean_WD,stddev_WD,f_NS = p

    m2s = np.linspace(0., 2., Nintegrate)
    dm2 = m2s[1] - m2s[0]

    integ_fac = integrand_factor(m2s, mf, m1)

    # White dwarf companion mixture component
    lower, upper = bounds_WD
    dist_WD = truncnorm((lower - mean_WD) / stddev_WD, (upper - mean_WD) / stddev_WD, loc=mean_WD, scale=stddev_WD)

    # Neutron star companion mixture component
    lower, upper = bounds_NS
    dist_NS = truncnorm((lower - mean_NS) / stddev_NS, (upper - mean_NS) / stddev_NS, loc=mean_NS, scale=stddev_NS)

    p_WD = (1-f_NS) * dist_WD.pdf(m2s)
    p_NS = f_NS * dist_NS.pdf(m2s)

    # Zero out when evaluating outside of allowed bounds (normally NaN)
    integ_fac[np.isnan(integ_fac)] = 0.
    p_WD[np.isnan(p_WD)] = 0.
    p_NS[np.isnan(p_NS)] = 0.

    # we approximate the integral using the trapezoidal rule
    integrand_WD = p_WD * integ_fac
    integrand_NS = p_NS * integ_fac

    p_WD = dm2/2. * (integrand_WD[0] + np.sum(2*integrand_WD[1:-1], axis=0) + integrand_WD[-1])
    p_NS = dm2/2. * (integrand_NS[0] + np.sum(2*integrand_NS[1:-1], axis=0) + integrand_NS[-1])

    return np.vstack((p_WD, p_NS))

def main(m1, mf, nsamples):

    file_url = "http://files.figshare.com/1720018/posterior_samples.txt"
    cache_path = "data"
    local_file = os.path.join(cache_path, "posterior_samples.txt")

    if not os.path.exists(cache_path):
        os.mkdir(cache_path)

    if not os.path.exists(local_file):
        print("Posterior sample file doesn't exist locally.")
        print("Downloading and caching to: {}".format(os.path.abspath(local_file)))

        # download and save
        f = urllib2.urlopen(file_url)

        with open(local_file, 'w') as f2:
            f2.write(f.read())

    else:
        print("Reading cached file from: {}".format(os.path.abspath(local_file)))

    samples = np.genfromtxt(local_file, delimiter=',', names=True)

    m2s = np.linspace(0, 2., 50)
    p_m2s = np.zeros((nsamples, len(m2s)))

    P_NS = np.zeros(nsamples)
    for i,p in enumerate(samples[:nsamples]):
        p_WD,p_NS = likelihood(p, mf, m1, bounds_WD)[:,0]
        P_NS[i] = p_NS / (p_WD + p_NS)

        p_m2s[i] = integrand_factor(m2s, mf, m1) * m2_func(p, mf, m1, bounds_WD, m2s)

    fig,axes = plt.subplots(2,1,figsize=(10,12))
    binw = 3.5*np.std(P_NS) / len(P_NS)**(1/3.)
    axes[0].hist(P_NS, bins=np.arange(0.,1.+binw,binw), normed=True)
    axes[0].set_xlabel(r"$P_{\rm NS}$")
    axes[0].axvline(np.mean(P_NS), alpha=0.5, lw=2., color='g')
    axes[0].axvline(scoreatpercentile(P_NS,16), alpha=0.5, lw=2., color='g', linestyle='dashed')
    axes[0].axvline(scoreatpercentile(P_NS,84), alpha=0.5, lw=2., color='g', linestyle='dashed')
    axes[0].set_xlim(0,max(P_NS)+0.05)

    axes[1].errorbar(m2s, np.mean(p_m2s,axis=0), np.std(p_m2s,axis=0),
                     marker='o', ecolor='#666666')
    # for i in np.random.randint(0,nsamples,100):
    #     axes[1].plot(m2s, p_m2s[i], marker=None, lw=2., color='#666666', alpha=0.25)
    # axes[1].plot(m2s, np.mean(p_m2s,axis=0), marker=None, lw=2., color='k')
    axes[1].set_xlabel(r"${\rm M}_2 [{\rm M}_\odot]$")

    print("Mean P_NS: {:.3f}".format(np.mean(P_NS)))
    print("Std. deviation P_NS: {:.3f}".format(np.std(P_NS)))
    print("Median P_NS: {:.3f}".format(np.median(P_NS)))
    print("16th percentile P_NS: {:.3f}".format(scoreatpercentile(P_NS,16)))
    print("84th percentile P_NS: {:.3f}".format(scoreatpercentile(P_NS,84)))

    plt.show()

if __name__ == '__main__':
    from argparse import ArgumentParser

    # Define parser object
    parser = ArgumentParser(description="")

    parser.add_argument("--m1", dest="m1", default=None, required=True,
                        type=float, help="Mass of the primary.")
    parser.add_argument("--mf", dest="mf", default=None, required=True,
                        type=float, help="Mass function.")
    parser.add_argument("--nsamples", dest="nsamples", default=1000,
                        type=int, help="Number of posterior samples to use.")

    args = parser.parse_args()

    warnings.simplefilter("ignore", RuntimeWarning)
    main(args.m1, args.mf, nsamples=args.nsamples)

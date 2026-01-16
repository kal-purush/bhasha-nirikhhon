"""
Models for continuum galaxy properties, as a function of intrinsic galaxy 
properties.

Phil Bull, 2020
"""
import numpy as np

def Lff_murphy(sfr, nu, T=1e4):
    """
    Free-free luminosity model, taken from Eq. 8 of Bonaldi et al. 
    (arXiv:1805.05222).
    
    Parameters
    ----------
    sfr : array_like
        Star-formation rate, in Msun/yr.
    
    nu : array_like
        Frequency, in GHz.
    
    T : float, optional
        Plasma temperature, in K. Default: 1e4.
    
    Returns
    -------
    L_synch : array_like, float
        Synchrotron luminosity in W Hz^-1.
    """
    gaunt = None # FIXME
    freq_fac = np.exp(-h*nu / (kB * T)) # FIXME
    return 3.75e19 * sfr * (T / 1e4)**0.3 * gaunt * freq_fac


def Lsynch_murphy(sfr, nu):
    """
    Synchrotron luminosity model, taken from Eq. 9 of Bonaldi et al. 
    (arXiv:1805.05222).
    
    Parameters
    ----------
    sfr : array_like
        Star-formation rate, in Msun/yr.
    
    nu : array_like
        Frequency, in GHz.
    
    Returns
    -------
    L_synch : array_like, float
        Synchrotron luminosity in W Hz^-1.
    """
    freq_fac = nu**(-0.85) / (1. + (nu / 20.)**0.5)
    return 1.9e21 * sfr * freq_fac
    
    
def Lsynch_mancuso(sfr, nu, beta=3.):
    """
    Modified synchrotron luminosity model, taken from Eq. 10 of Bonaldi et al. 
    (arXiv:1805.05222).
    
    Parameters
    ----------
    sfr : array_like
        Star-formation rate, in Msun/yr.
    
    nu : array_like
        Frequency, in GHz.
    
    beta : float, optional
        Spectral index in correction factor. Default: 3.
    
    Returns
    -------
    L_synch : array_like, float
        Synchrotron luminosity in W Hz^-1.
    """
    Ls = Lsynch_murphy(sfr, nu)
    Lstar = 0.086 * Lsynch_murphy(sfr=1., nu=nu)
    x = Lstar / Ls
    return Lstar / (x**beta + x)


def Lsynch(sfr, nu, z, beta=3.):
    """
    Redshift-dependent synchrotron luminosity model, taken from Eq. 11 of 
    Bonaldi et al. (arXiv:1805.05222).
    
    Parameters
    ----------
    sfr : array_like
        Star-formation rate, in Msun/yr.
    
    nu : array_like
        Frequency, in GHz.
    
    z : float
        Redshift.
    
    beta : float, optional
        Spectral index in correction factor. Default: 3.
    
    Returns
    -------
    L_synch : array_like, float
        Synchrotron luminosity in W Hz^-1.
    """
    # Assumes 'log' in Bonaldi Eq. 11 is natural log
    z_fac = np.exp(2.35 * (1. - (1. + z)**(-0.12)))
    return z_fac * Lsynch_mancuso(sfr=sfr, nu=nu, beta=beta)


def R_sfg_halflight(mstar, alpha=0.115, beta=0.898, gamma=0.199, M0=3.016e10):
    """
    Half-light radius of star-forming galaxies, assuming exponential profile. 
    Taken from Eq. 15 of Bonaldi et al. (arXiv:1805.05222).
    
    Parameters
    ----------
    mstar : array_like
        Stellar mass, in Msun.
    
    alpha : float, optional
        Scaling parameter. Default: 0.115.
    
    beta : float, optional
        Second scaling parameter. Default: 0.898.
    
    gamma : float, optional
        Overall amplitude parameter. Default: 0.199.
    
    M0 : float, optional
        Reference mass in correction factor, in Msun. Default: 3.016e10.
    
    Returns
    -------
    R : array_like, float
        Half-light radius of galaxy, in kpc.
    """
    return gamma * mstar**alpha * (1. + mstar/M0)**(beta - alpha)
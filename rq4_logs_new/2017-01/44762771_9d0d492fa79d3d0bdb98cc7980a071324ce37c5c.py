import numpy as np
from scipy.interpolate import interp1d
from scipy.io import readsav

import re

from astropy.cosmology import FlatLambdaCDM
from astropy import units as u
import astropy 

from matplotlib.pylab import *
import matplotlib.pyplot as plt 
from matplotlib.ticker import FuncFormatter
from matplotlib.ticker import NullFormatter

rcParams['figure.figsize'] = (10,8)
rcParams['font.size'] = 22

import sys
sys.path.append('/Users/earnric/Google Drive/ASU/Codes/PythonCode/modules')
import loadfilt as lf
import lymanAbs as lyA

import itertools
import os
import subprocess
import glob
import gc
import linecache

#
# Read in Schaerer and SB99 luminosity files and generate flux in filter.
# These tables only consider Ly-forest absorption -- and redshift.
# User needs to apply reddening.
#

def buildFilterFluxFiles():
    jwstFilters   = lf.loadJWSTFilters(suppress=True)
    hubbleFilters = lf.loadHubbleFilters(suppress=True)
    lamRange = np.logspace(1.95,5.7,5500)

    # Create a header for the output files describing the columns     
    header = [i for i in itertools.chain(['LogAge','redshift'],
                                         [jFilt for jFilt in jwstFilters],
                                         [hFilt for hFilt in hubbleFilters])]
    
    redshifts = [5.0,5.5,6.0,6.5,7.0,7.5,8.0,8.5,9.0,9.5,10.0,11.0,12.0,13.0,14.0]

    schaererPath = '/Users/earnric/Research/Research-Observability/Software-Models/Schaerer/'
    schaererDirs = ['pop3_TA/','pop3_TE/','e-70_mar08/','e-50_mar08/']
    Zs           = [0.0, 0.0, 1.0e-7, 1.0e-5]
    schaererPopFilePattern  = 'pop3_ge0_log?_500_001_is5.[0-9]*' # is5 files have ages in step of 1 Myr
    schaererLowZFilePattern = 'e-?0_sal_100_001_is2.[0-9]*'      # is2 files have ages in step of 0.05 dex

    # Load the schaerer files... 
    for i, (Z, schaererDir,anArray) in enumerate(zip(Zs,schaererDirs,arrayNames)):
        if schaererDir.startswith('pop3'):
            schaererFilePattern = schaererPath + schaererDir + schaererPopFilePattern  # Pop III files, 1 Myr spacing
        else:
            schaererFilePattern = schaererPath + schaererDir + schaererLowZFilePattern # Low Z files, 0.05 dex spacing

        schaererFiles   = glob.glob(schaererFilePattern)  # All the files in the dir... 
        schaererFiles   = [a for a in schaererFiles if not re.search('\.[1-2][0-9][b]*$', a)] # remove .1? and .2? files
        schaererAges    = np.array([linecache.getline(file,13) for file in schaererFiles])    # Get the line with the (log) age... 
        schaererAges    = np.array([float(sa[30:]) for sa in schaererAges],dtype=float)       # Log age starts at position 30
        schaererData    = np.array([np.loadtxt(file,skiprows=16) for file in schaererFiles])
        ageSortIndxes   = schaererAges.argsort()          # Array of indices to sort things by age...

        # The following builds an array of arrays (one for each age) with each array's entries:
        # [log age, waveln, lum/A]
        allSchaererData = np.array([np.insert(sed[:,[0,2]],[0],[[age] for ii in range(0,len(sed))], axis=1) 
                                    for age,sed in zip(schaererAges[ageSortIndxes], schaererData[ageSortIndxes])])
        allSchaererData = allSchaererData.reshape(len(allSchaererData)*len(allSchaererData[0]),3)
        if i == 0:
            pop3TA = allSchaererData # may need a np.copy(...) here... ??
        elif i == 1:
            pop3TE = allSchaererData
        elif i == 2:
            Zem7 = allSchaererData
        elif i == 3:
            Zem5 = allSchaererData

    schaererList = [pop3TA, pop3TE, Zem7, Zem5]

    for schaererData in schaererList:
        # Get the wavelength & Luminosity/s/Ang
        wavelns = allSchaererData[:,2] * u.Angstrom
        LperA   = allSchaererData[:,3] * u.erg / u.second / u.angstrom
        # Convert to freq & Lumin/s/Hz
        freq = (wavelns[::-1]).to(u.Hz, equivalencies=u.spectral())
        LperHz = (LperA * wavelns**2/astropy.constants.c).to(u.erg/u.s/u.Hz)[::-1] # Let astropy do units...
        for z in redshifts:
            absorb = lyA.lyTau(z) # Create a lyman forest absorption function for z...
            rsWaveln, rsLperA  = lyA.rsSEDwavelen(wavelns, LperA,z) # Redshift SED in wavelen
            rsFreq,   rsLperHz = lyA.rsSEDfreq(freq, LperHz,z)      # Redshift SED in freq
            lyForFluxHz = (rsLperHz * absorb(rsWaveln[::-1]))
            lyForFluxA  = (rsLperA  * absorb(rsWaveln))
            for aFilt in jwstFilters:
                flux = np.trapz(jwstFilters[aFilt](rsWaveln[::-1])*lyForFluxHz/rsFreq,rsFreq)
                print("Flux in JWST {} = {:.3e}".format(aFilt,flux))

    # SB99 format:     TIME [YR]    WAVELENGTH [A]   LOG TOTAL  LOG STELLAR  LOG NEBULAR  [ERG/SEC/A]
    # REMEMBER, SB99 data is for a population of 1e6 M_sun
    for i, (Z, SB99Dir,anArray) in enumerate(zip(Zs,SB99Dirs,arrayNames)):
        SB99FilePattern = SB99Path + SB99Dir + SB99FilePat 
        SB99Files   = glob.glob(SB99FilePattern)  # All the files in the dir... should be one!
        if len(SB99Files) != 1:
            print('Error: too many files in an SB99 dir! - ',SB99Path + SB99Dir)
            sys.exit()
        SB99Data    = np.loadtxt(SB99Files[0],skiprows=6)
        if i == 0:
            SB990004 = np.dstack((np.log10(SB99Data[:,0]),SB99Data[:,1],
                                  10**(SB99Data[:,2]-6.0))).reshape(len(SB99Data[:,1]),3)
        elif i == 1:
            SB99004 = np.dstack((np.log10(SB99Data[:,0]),SB99Data[:,1],
                                 10**(SB99Data[:,2]-6.0))).reshape(len(SB99Data[:,1]),3)
        elif i == 2:
            SB99008 = np.dstack((np.log10(SB99Data[:,0]),SB99Data[:,1],
                                 10**(SB99Data[:,2]-6.0))).reshape(len(SB99Data[:,1]),3)
        elif i == 3:
            SB9902 = np.dstack((np.log10(SB99Data[:,0]),SB99Data[:,1],
                                10**(SB99Data[:,2]-6.0))).reshape(len(SB99Data[:,1]),3)
    # We now have:
    # [[log age, waveln, flux], [], ...]

    return pop3TA,pop3TE,Zem7,Zem5,SB990004,SB99004,SB99008,SB9902

## SBPath  = '/Users/earnric/Research/Research-Observability/Software-Models/STARBURST99/STARBURST99-runs/'
## SBDirs = np.array(['padova0004','padova004','padova008','padova02'])


## #
## # Convert the extension number (31,...) to 'log age' of the sed 
## # For the is2 files, we can map the extension to log age via: 0.05 ext + 2.45 = log age
## #
## sedAgeZ02 = np.multiply(sedFile02exts, 0.05) + 2.45 # just keep 2 decimal places... 
## # sort by age -- the following generates an ordered list of INDICES that indexes the arrays in age-order
## Z02AgeSortIndx = sedAgeZ02.argsort()

## Z = 0.02 # This is the metallicity of the file we're working with
## #
## # BUILD A SORTED (in time) ARRAY
## # Prepend the age onto each sed array such that it has the form:
## # [[[log age,  lambda, total L/A], [log age,  lambda2, total L/A],...],
## #  [[log age2, lambda, total L/A], [log age2,  lambda2, total L/A], ...],
## # ... ]
## #
## Zm02 = [np.insert(sed[:,[0,2]],[0],[[age] for ii in range(0,len(sed))], axis=1) 
##        for age,sed in zip(sedAgeZ02[Z02AgeSortIndx], sedsZ02[Z02AgeSortIndx])]

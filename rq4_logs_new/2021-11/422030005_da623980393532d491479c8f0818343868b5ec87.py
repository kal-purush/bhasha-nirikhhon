#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 08:43:23 2021

@author: joshuaumiamaka
"""

from astropy.io import fits
import numpy as np
import random
import math
import sys
import sncosmo

#initailizing input
f = fits.open(sys.argv[1])
data = f[0].data
header = f[0].header
f.close()

#python this_script.py 
print(sys.argv)

#initializing data
model = sncosmo.Model(source='hsiao')
REDSHIFT = f["IMAGE_PSF"].header["REDSHIFT"]
model.set(z=REDSHIFT, t0=0., amplitude = 1.e-10)
model.bandmag('F200W', 'ab', time=0.)
#brightnes(flux) = 10^(-0.4*(ABmag -31.4))
model.set(z=REDSHIFT)
mabs = -19.1
model.set_source_peakabsmag(mabs, 'bessellb', 'ab')


#area = 1.  # area in square degrees
#tmin = 56175.  # minimum time
#tmax = 56225.  # maximum time
#zmax = 0.7
#redshifts = list(sncosmo.zdist(0., zmax, time=(tmax-tmin), area=area))
#from numpy.random import uniform, normal
#params = [{'x0':1.e-5, 'x1':normal(0., 1.), 'c':normal(0., 0.1),
 #          't0':uniform(tmin, tmax), 'z': z}
 
 #defining the coordinates
i = None
j = None
 
#method for finding coordinates
def pk_coord():
    #while loop chooses the coordinates to place supernovae
    stop = 1
    while stop == 1:
        #randomly pick coordinates
        i = random.randint(0,len(data) - 1)
        j = random.randint(0,len(data) - 1)
        num = data[i,j]/data.max()
        randNum = random.random()
        #if the random number is less than fraction then use coordinates
        if randNum < num:
            stop = 0
        #if the ratio is 1 then place SNe at coordinates
        elif num == 1:
            stop = 0
        #do not place the SNe in the corner of the image
        elif data[i,j] == 0:
            stop = 1
        #if coordinates do not work, then find new coordinates
        else:
            stop = 1
    return i, j

yes = np.random.random()
i,j = pk_coord()

#while loop chooses the coordinates to place supernovae

print(i,j)
filter = ""
file = ""
end = 0

#while loop goes through all of the different filters 
#all in one position
end = 0
while end != 4:
    #starts with F115 filter
    if end == 0:
        filter = 'F115W'
        file = "/Users/joshuaumiamaka/Downloads/F115W.fits"
    if end == 1:
        filter = 'F150W'
        file = "/Users/joshuaumiamaka/Downloads/F150W.fits"
    if end == 2:
        filter = 'F277W'
        file = "/Users/joshuaumiamaka/Downloads/F277W.fits"
    if end == 3:
        filter = 'F444W'
        file = "/Users/joshuaumiamaka/Downloads/F444W.fits"
        
    if filter == "F115W":
        f = fits.open(sys.argv[1].replace('f115w', filter.lower()))
    if filter == "F150W":
        f = fits.open(sys.argv[1].replace('f115w', filter.lower())
    if filter == "F277W":
        f = fits.open(sys.argv[1].replace('f115w', filter.lower())
    if filter == "F444W":
        f = fits.open(sys.argv[1].replace('f115w', filter.lower())
    data = f[0].data
    #WAS F200W
    model.bandmag(filter , 'ab', time=0.)
    print(model.bandmag(filter , 'ab', time=0.))
    print(REDSHIFT)
    print(header)

    magb= model.bandmag(filter, 'ab', time=0.)
    e_s= 10**((magb-header['ABZP'])/-2.5)
    print(e_s)
    
    #opens image 
    h = fits.open(file)
    PSF = h["DET_SAMP"].data
    
    x = 0
    y = 0
    center = int((len(PSF)-1)/2)
    x=center
    y=center
    print("x, y", x,y)
    print("i,j", i,j)
    print("e_s", e_s)
    
    f[0].header["SN_i"] = i
    f[0].header["SN_j"] = j
    f[0].header["SN_" + filter] = magb
    
    PSF = PSF[x - 5: x + 5, y - 5: y + 5]
    if yes >= 0.5:      
        f[0].header["placed"] = 'yes'
        data[i - 5: i + 5, j - 5: j + 5] += PSF*e_s
    
    #figure out what parts of the path are unique and add it her for "-".join
    f.writeto("file" + "-".join(sys.argv[1].split("/"[:3]) + filter + ".fits", clobber = True)
    f.close()
    old_fl = "/Users/joshuaumiamaka/Downloads/hlsp_vela_jwst_nircam_vela01_f150w_v3_sim/cam01/jwst/nircam/f150w/hlsp_vela_jwst_nircam_vela01-cam01-a0.250_f150w_v3_sim.fits"
    new_fl = "file" + filter + ".fits"
    assert old_fl != new_fl
    
    end = end + 1
import subprocess
subprocess.getoutput("/Applications/ds9 new_file6_" + "*" + ".fits")
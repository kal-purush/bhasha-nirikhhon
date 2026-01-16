import matplotlib.pylab as plt
import numpy as np
import os.path
from netCDF4 import Dataset
import glob

# path to ascii data
datdir = '/media/suomi/My Passport/ABOA_2010-2011/2010-Antarct/00DATA/AWS5/Flux/10m/'
# path to netcdf data
outdir = '/media/suomi/My Passport/ABOA_2010-2011/2010-Antarct/00DATA/AWS5/Flux/nc/'

# ascii file names
fnames = np.sort(np.array(glob.glob(datdir+'10m-101225*.dat')))
hgt = 10

# Loop over ascii files:
for fname in [fnames[0]]:
    #
    # Read data
    dat = np.genfromtxt(fname)
    [time,x,y,z,T] = np.transpose(np.genfromtxt(fname,usecols=(0,3,5,7,9)))
    #
    tle = fname[-15:-4]
    print tle
    #
    # create/open nc-file
    if os.path.isfile(outdir+'AWS5_sonic_20hz_10m_20'+tle+'.nc'):
        nc = Dataset(outdir+'AWS5_sonic_20hz_10m_20'+tle+'.nc','r+',format='NETCDF4')
    else:
        nc = Dataset(outdir+'AWS5_sonic_20hz_10m_20'+tle+'.nc','w',format='NETCDF4')
    #
    # create dimension
    dimkeys = nc.dimensions.keys()
    if ('dim') not in dimkeys:
        nc.createDimension('dim',len(time))
    #
    # create variables
    varkeys = nc.variables.keys()
    if ('time') in varkeys:
        time_save = nc.variables['time']
    else:
        time_save = nc.createVariable('time', 'i8', ('dim'))
    if ('X') in varkeys:
        x_save = nc.variables['X']
    else:
        x_save = nc.createVariable('X', 'f4', ('dim'))
    if ('Y') in varkeys:
        y_save = nc.variables['Y']
    else:
        y_save = nc.createVariable('Y', 'f4', ('dim'))
    if ('Z') in varkeys:
        z_save = nc.variables['Z']
    else:
        z_save = nc.createVariable('Z', 'f4', ('dim'))
    if ('T') in varkeys:
        T_save = nc.variables['T']
    else:
        T_save = nc.createVariable('T', 'f4', ('dim'))
    #
    # save data into the nc-file:
    time_save[:] = time
    x_save[:] = x
    y_save[:] = y
    z_save[:] = z
    T_save[:] = T
    nc.close()
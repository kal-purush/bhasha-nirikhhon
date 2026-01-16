'''
 # @ Author: Your name
 # @ Create Time: 2022-08-17 16:49:06
 # @ Modified by: Your name
 # @ Modified time: 2022-08-17 16:50:21
 # @ Description:
 '''

# ---------------------------------- imports --------------------------------- #
import xarray as xr
from multiprocessing import Pool
from glob import glob

# ---------------------------------------------------------------------------- #

def create_ascat(yr):
    pdir='data/ASCAT'
    yr = str(yr)
    paths = glob(pdir+'/'+yr+'*.nc')
    DATA = []
    for p in paths:
        try:
            data = xr.open_dataset(p, chunks='auto').squeeze()
            data = data[['eastward_wind','northward_wind']]
            data = data.sel(latitude=slice(-50,-5), longitude=slice(-100,-65))
            DATA.append(data)
        except:
            pass
    DATA = xr.concat(DATA, 'time')
    return DATA

def create_quikscat(yr):
    yr = str(yr)
    pdir='data/QuikSCAT'
    paths = glob(pdir+'/'+yr+'*.nc')
    DATA = []
    for p in paths:
        try:
            data = xr.open_dataset(p, chunks='auto').squeeze()
            data = data[['eastward_wind','northward_wind']]
            data = data.sel(latitude=slice(-50,-5), longitude=slice(-100,-65))
            DATA.append(data)
        except:
            pass
    DATA = xr.concat(DATA, 'time')
    return DATA

ascat = Pool(processes=10).map(create_ascat,range(2007,2022+1,1))
ascat = xr.concat(ascat,'time')
quikscat = Pool(processes=10).map(create_quikscat,range(1999,2009+1,1))
quikscat = xr.concat(quikscat,'time')

common_time = list(set(ascat.time.values) & set(quikscat.time.values))
common_time = sorted(common_time)

ascat.sel(time=common_time).stack()
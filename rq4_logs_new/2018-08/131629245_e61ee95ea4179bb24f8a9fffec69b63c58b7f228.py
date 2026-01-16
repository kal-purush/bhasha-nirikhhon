"""

    create input data for MITgcm model

"""

import numpy as np
import modeldata

dtype = '>f8'

# If True, will plot all met. fields
makeplot = True

data = modeldata.getdata()
period = 3600.0  # delta T between records in EXF input files
Tday = 86400.0

# array type is 64 bit float in big endian
datat = np.dtype('>f8')

xwindamp = -1.0
ywindamp = 0.0
aqhconst = 0.01
swdownconst = 500.
lwdownconst = 500.
atempconst = 278.15
atempamp = 10.0
qfluxconst = 0.

# ntimes = int((data.endTime - data.startTime)/period)
ntimes = int(Tday / period)
print("Using %i time steps" % ntimes)

xwind = np.ones( (data.nx, data.ny, ntimes), dtype=datat)
ywind = np.ones( (data.nx, data.ny, ntimes), dtype=datat)
airtemp = np.ones( (data.nx, data.ny, ntimes), dtype=datat)
qfluxnet = np.ones( (data.nx, data.ny, ntimes), dtype=datat)
aqh = np.ones( (data.nx, data.ny, ntimes), dtype=datat)
swdown = np.ones( (data.nx, data.ny, ntimes), dtype=datat)
lwdown = np.ones( (data.nx, data.ny, ntimes), dtype=datat)

X = np.array([np.sum(data.delX[:i]) for i in range(data.nx)])
Y = np.array([np.sum(data.delX[:i]) for i in range(data.ny)])
Z = np.array([np.sum(data.delX[:i]) for i in range(data.nz)])

Xgrid, Ygrid = np.meshgrid(X, Y)

sigmat = 24*60*60

for i in range(ntimes):
# Not sure why we need Ygrid.T to match old results, but we seem to
    t = i*period
    phs = 2*np.pi*t/Tday
    print("Time: %s    Phase: %s" % (t, phs)) 
    f = 0.5*(1.0 - np.tanh((Ygrid.T - 12500.)/2000.))
#   xwind[:, :, :] = np.reshape(f * xwindamp, (X.shape[0], Y.shape[0], 1))
    xwind[:, :, i] = np.cos(phs) * xwindamp
    ywind[:, :] = 0
    # airtemp[:, :] = np.reshape(274.15 - f * atempamp, (X.shape[0], Y.shape[0], 1))
    airtemp[:, :, i] = atempconst - atempamp * np.cos(phs)
    qfluxnet[:, :] = qfluxconst
    aqh[:, :] = aqhconst
    swdown[:, :, i] = swdownconst/2 * (1 -  np.cos(phs))
    lwdown[:, :, i] = 50 + lwdownconst/2 * (1 -  np.cos(phs))
    ywind[:, :] = ywindamp

print(airtemp[1, 1, :])
print(xwind[1, 1, :])
print(swdown[1, 1, :])

# Write to file in fortran order, with data type '>f8'

fid = open("x_wind.bin", "wb")
xwind.astype(dtype).ravel(order='F').tofile(fid)
fid.close()

fid = open("y_wind.bin", "wb")
ywind.astype(dtype).ravel(order='F').tofile(fid)
fid.close()

fid = open("airtemp.bin", "wb")
airtemp.astype(dtype).ravel(order='F').tofile(fid)
fid.close()

# fid = open("Qflux.bin", "wb")
# qfluxnet.astype(dtype).ravel(order='F').tofile(fid)
# fid.close()

fid = open("aqh.bin", "wb")
aqh.astype(dtype).ravel(order='F').tofile(fid)
fid.close()

fid = open("swdown.bin", "wb")
swdown.astype(dtype).ravel(order='F').tofile(fid)
fid.close()

fid = open("lwdown.bin", "wb")
lwdown.astype(dtype).ravel(order='F').tofile(fid)
fid.close()

if makeplot:
    import matplotlib
    matplotlib.use('Agg')
    from matplotlib import pyplot as plt
    
    fig, axs = plt.subplots(nrows = 6, figsize = (8, 8), sharex = True)
    
    times = period * np.arange(ntimes) / 86400.0
    
    axs[0].plot(times, airtemp[0, 0, :], color = 'r')
    axs[1].plot(times, aqh[0, 0, :], color = 'g')
    axs[2].plot(times, swdown[0, 0, :], color = 'b')
    axs[3].plot(times, lwdown[0, 0, :], color = 'm')
    axs[4].plot(times, xwind[0, 0, :], color = 'k')
    axs[5].plot(times, ywind[0, 0, :], color = 'c')

    ylabels = ['Air Temp (C)', 'Humidity (Kg/Kg)', 'Shortwave (W/m^2)',
                    'Longwave (W/m^2)', 'U (m/s)', 'V (m/s']

    for i, ylabel in enumerate(ylabels):
        axs[i].set_ylabel(ylabel, fontsize=8, rotation='horizontal', labelpad=10, ha='right')
        axs[i].grid()
    axs[-1].set_xlabel('Time (days)')
    plt.tight_layout()
    fig.savefig('meterological_forcing.png', dpi = 600)
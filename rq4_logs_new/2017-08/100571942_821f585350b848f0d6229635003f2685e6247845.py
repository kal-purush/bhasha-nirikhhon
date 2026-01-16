import numpy as np
import scipy.stats #import binned_statistic_2d
import math
import numpy.ma as ma
import pickle
import matplotlib.pyplot as plt
#~ import matplotlib.colormap as cp

import young_star_candidates as ys

# Convergence point
# This is solar apex; replace it with young stars
#~ from astropy.coordinates import SkyCoord
#~ from astropy import units as u
#~ c = SkyCoord(l=50.2*u.degree, b=24.7*u.degree, frame='galactic')
#~ RA_CP=c.icrs.ra.value
#~ DEC_CP=c.icrs.dec.value

# Convergence point coordinates
# This is Solar apex:
RA_CP=270.0
DEC_CP=30.0

# Read the data
pkl_file = open('gaia_2mass_for_funnelweb.pkl', 'rb')
d = pickle.load(pkl_file)
pkl_file.close()

result=[]
for x in d:
	# pmra = mu_alpha* (meaning that cos(delta) has already been taken into account)
	
	# Determine angle theta between the two coordinate systems
	theta=ys.determine_theta(alpha_cp=RA_CP, delta_cp=DEC_CP, alpha=x['ra'], delta=x['de'])
	
	# Method 1: Components in a new coordinate system
	#~ pm_parallel, pm_perp = ys.components_in_a_new_system(theta=theta, pmRA=x['pmra'], pmDE=x['pmdec'])
	
	# Method 2: Rotate vector for angle theta (rotation is done for -theta)
	rotated_vector=ys.rotate_vector(vec=np.array([x['pmra'], x['pmdec']]), theta=theta)
	pm_parallel=rotated_vector[0]
	pm_perp=rotated_vector[1]
	
	pm_parallel /= np.sin(np.deg2rad(theta))
	
	# Reduced proper motions
	# 1e-3: convert from mas/yr to arcsec/yr
	gmag=x['phot_g_mean_mag']
	#~ H_parallel = gmag+5.0+5.0*np.log10(pm_parallel*1e-3)
	#~ H_perp = gmag+5.0+5.0*np.log10(pm_perp*1e-3)
	
	# Take absolute values of proper motion
	H_parallel = gmag+5.0+5.0*np.log10(np.abs(pm_parallel)*1e-3)
	H_perp = gmag+5.0+5.0*np.log10(np.abs(pm_perp)*1e-3)
	
	result.append([pm_parallel, pm_perp, x['ra'], x['de'], H_parallel, H_perp, gmag-x['k_m']])

amp=100

# Parallel component: distribution in the sky
result=np.array(sorted(result, key=lambda x: x[0]))
fig=plt.figure()
ax=fig.add_subplot(211)
cb=ax.scatter(result[:,2], result[:,3], c=result[:,0], vmin=-amp, vmax=amp, s=5, edgecolor='none')
c=plt.colorbar(cb)
c.set_label('pm parallel [mas/yr]')
ax.set_xlabel('RA')
ax.set_ylabel('DEC')

result=np.array(sorted(result, key=lambda x: x[0], reverse=True))
ax=fig.add_subplot(212)
cb=ax.scatter(result[:,2], result[:,3], c=result[:,0], vmin=-amp, vmax=amp, s=5, edgecolor='none')
c=plt.colorbar(cb)
c.set_label('pm parallel [mas/yr]')
ax.set_xlabel('RA')
ax.set_ylabel('DEC')

# Perp component
result=np.array(sorted(result, key=lambda x: x[1]))
fig=plt.figure()
ax=fig.add_subplot(211)
cb=ax.scatter(result[:,2], result[:,3], c=result[:,1], vmin=-amp, vmax=amp, s=5, edgecolor='none')
c=plt.colorbar(cb)
c.set_label('pm perp [mas/yr]')
ax.set_xlabel('RA')
ax.set_ylabel('DEC')

result=np.array(sorted(result, key=lambda x: x[1], reverse=True))
ax=fig.add_subplot(212)
cb=ax.scatter(result[:,2], result[:,3], c=result[:,1], vmin=-amp, vmax=amp, s=5, edgecolor='none')
c=plt.colorbar(cb)
c.set_label('pm perp [mas/yr]')
ax.set_xlabel('RA')
ax.set_ylabel('DEC')

# PM parallel vs PM perpendicular
fig=plt.figure()
ax=fig.add_subplot(111)
cb=ax.scatter(result[:,1], result[:,0], s=5)
ax.set_xlabel('pm perp [mas/yr]')
ax.set_ylabel('pm parallel / sin(theta) [mas/yr]')
ax.axhline(y=0, c='k')
ax.axvline(x=0, c='k')
ax.set_xlim(-100, 100)
ax.set_ylim(-1000, 1000)

# PM parallel vs PM perpendicular vs color



# H_parallel vs G-K
fig=plt.figure()
ax=fig.add_subplot(121)
#~ cb=ax.scatter(result[:,6], result[:,4], c=result[:,2], vmin=0, vmax=200, s=5, edgecolor='none', cmap=plt.cm.RdYlGn)
cb=ax.scatter(result[:,6], result[:,4], c=result[:,1], vmin=-50, vmax=50, s=5, edgecolor='none', cmap=plt.cm.RdYlGn)
c=plt.colorbar(cb)
c.set_label('pm parallel')
ax.set_xlabel('G-K')
ax.set_ylabel('H parallel')
ax.set_xlim(-1, 5)
ax.set_ylim(20, -15)

def shift_edges(e=False): # to plot histograms
	d=e[1]-e[0]
	e2=[x+d/2.0 for x in e]
	return e2[:-1]

xbins=100
ybins=100
xlim=[-1, 5]
ylim=[-15, 20]
hist, xedges, yedges, binnumber = scipy.stats.binned_statistic_2d(result[:,4], result[:,6], result[:,6], statistic='count', bins=[ybins, xbins], range=[ylim, xlim])
hist=ma.array(hist, mask=hist<0.000001)
hist=np.log10(hist)
X, Y = np.meshgrid(yedges, xedges)
#~ shift edges
xedges=shift_edges(xedges)
yedges=shift_edges(yedges)
cs = ax.contour(yedges, xedges, hist, colors='k')

# H_perp vs G-K
ax=fig.add_subplot(122)
cb=ax.scatter(result[:,6], result[:,5], c=result[:,1], vmin=-50, vmax=50, s=5, edgecolor='none', cmap=plt.cm.RdYlGn)
c=plt.colorbar(cb)
c.set_label('pm parallel')
ax.set_xlabel('G-K')
ax.set_ylabel('H perpendicular')
ax.set_xlim(-1, 5)
ax.set_ylim(20, -15)

hist, xedges, yedges, binnumber = scipy.stats.binned_statistic_2d(result[:,5], result[:,6], result[:,6], statistic='count', bins=[ybins, xbins], range=[ylim, xlim])
hist=ma.array(hist, mask=hist<0.000001)
hist=np.log10(hist)
X, Y = np.meshgrid(yedges, xedges)
#~ shift edges
xedges=shift_edges(xedges)
yedges=shift_edges(yedges)
cs = ax.contour(yedges, xedges, hist, colors='k')


# H_perp vs H_parallel
fig=plt.figure()
ax=fig.add_subplot(111)
result=np.array(sorted(result, key=lambda x: x[6]))
cb=ax.scatter(result[:,4], result[:,5], c=result[:,6], s=5, edgecolor='none', cmap=plt.cm.RdYlGn, vmin=0.5, vmax=3)
c=plt.colorbar(cb)
c.set_label('G-K')
ax.set_xlabel('H parallel')
ax.set_ylabel('H perpendicular')
#~ ax.set_xlim(-1, 5)
#~ ax.set_ylim(20, -15)

# G-K histogram
#~ fig=plt.figure()
#~ ax=fig.add_subplot(111)
#~ ax.hist(result[:,6], bins=100)

plt.show()
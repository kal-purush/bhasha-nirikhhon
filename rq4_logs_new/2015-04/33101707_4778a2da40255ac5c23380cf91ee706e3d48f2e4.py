"""
This is a collection of functions which I have found useful on previous 
occasions
"""

import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as ss 

def KDEplot(Measure,Filename,NResample=None):
	"""This function takes a 2-by-N numpy array which encodes a random measure.
	the first row is the support of of the measure and the second the weights 
	associated with each point. the measure is sampled to obtain an iid sample 
	and then gaussian kde is used to approximate the density. a plot of the 
	density is saved under the given filename
	Args:
		Measure is an array (random measure) as described above
		Filename is the name given to the saved plot
		NResample is the number of points to resample
	"""
	if NResample == None:
		Size = len(Measure[0,:])
	else:
		Size = NResample
	# resample the given measure to get an iid sample
	Resample = np.random.choice(Measure[0,:], size=Size, p=Measure[1,:])
	# generate a gaussian KDE approximation to the density
	GKDE = ss.gaussian_kde(Resample)
	# generate a plot of the figure and save it as Filename
	xMin = Measure[0,:].min()
	xMax = Measure[0,:].max()
	X = np.linspace(xMin, xMax, num=100)	# domain
	Y = GKDE(X)								# density values

	plt.figure()
	plt.plot(X,Y)
	plt.savefig(Filename)



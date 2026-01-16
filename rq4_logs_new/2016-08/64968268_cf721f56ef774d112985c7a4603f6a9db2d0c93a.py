from __future__ import division
import numpy as np
import matplotlib.pyplot as pp
import astropy 
from astropy.io import fits
from astropy.io import ascii
from scipy import stats
from astropy.io.fits import getval
from scipy.spatial import cKDTree
import sys
from scipy.stats import beta 

folder_names = ['CFHQS-J003311.40-012524.9','NDWFS-J142516.30+325409.0','QSO-J0005-0006', 'SDSS-J012958.51-003539.7', 'SDSS-J020332.39+001229.3', 'SDSS-J205406.42-000514.8']
# How far to search for objects
dist_bound = 1.0 / 0.06
# How many sample points to try
n_samps = 500

def rand_numb(folder_name): 
    # Name the files first but add folder_name so that can run
    f160w_file = '/Users/Victoria/Documents/ASU/Research/Part_2_Hubble_Images/all_quasar_images/{folder_name}/F160W/{folder_name}_F160W_drz_sci.fits'.format(folder_name=folder_name)
    xsize = fits.getval(f160w_file,'NAXIS1')
    ysize = fits.getval(f160w_file,'NAXIS2')
    randx = stats.uniform.rvs(1,xsize, size = n_samps)
    randy = stats.uniform.rvs(1,ysize, size = n_samps)   
    # call on the two catalogs f160W and f125W
    qcat_f160W = '/Users/Victoria/Documents/ASU/Research/Part_2_Hubble_Images/all_quasar_images/{folder_name}/F160W/{folder_name}_F160W_phot.cat'.format(folder_name=folder_name) 
    cat_f160W = ascii.read(qcat_f160W,format = 'sextractor')
        
    qcat_f125W = '/Users/Victoria/Documents/ASU/Research/Part_2_Hubble_Images/all_quasar_images/{folder_name}/F125W/{folder_name}_F125W_phot.cat'.format(folder_name=folder_name) 
    cat_f125W = ascii.read(qcat_f125W,format = 'sextractor')
    
        
    # call on the x and y pixel count
    xy = np.column_stack([cat_f160W['X_IMAGE'], cat_f160W['Y_IMAGE']])
    kd = cKDTree(xy)
        
    # do the random number generator 
    test_pts = np.column_stack([randx, randy])
    dists,ind = kd.query(test_pts,k=1, distance_upper_bound= dist_bound)
        
    # make cut based on those with found neighbors
    found_cat = cat_f160W[ind[dists < np.inf]]
    cat_f125W = cat_f125W[ind[dists < np.inf]]
    successes = len(found_cat)
    failures = test_pts.shape[0] - successes
        
    # Check the definition of beta distribution, the +1 is basically so the
    # math works even in the case where successes or failures is 0
    found_dist = beta(successes + 1, failures + 1)
    # This gives us a 2-tuple of the 68% confidence interval
    # i.e. there is 68% probability the value falls between (min, max)
    # 68% is the usally-quoted "1-sigma" statistical uncertainty
    found_frac = found_dist.interval(0.68)

    print 'Without Color Criteria'
    print 'Samples: {:d} Matched: {:d} Frac: {:0.3g}-{:0.3g}'.format(successes+failures, successes, found_frac[0], found_frac[1])
        
    # take out the bad magnitudes 
    phot_mask = (found_cat['MAG_AUTO'] < 26.44)
        
    # Repeat beta above for magnitudes
    found_cat = found_cat[phot_mask]
    cat_f125W = cat_f125W[phot_mask]
    successes = len(found_cat)
    failures = test_pts.shape[0] - successes
    found_dist = beta(successes + 1, failures + 1)
    found_frac = found_dist.interval(0.68)

    print 'With Color Criteria'
    print 'Samples: {:d} Matched: {:d} Frac: {:0.3g}-{:0.3g}'.format(successes+failures, successes, found_frac[0], found_frac[1])
        
    #print((len(found_cat), len(cat_f125W))
        
    # Add color cut 
    color_jh = cat_f125W['MAG_AUTO'] - found_cat['MAG_AUTO']
    color_mask = color_jh < 0.5
    found_cat = found_cat[color_mask]
        
    # Repeat Beta
    successes = len(found_cat)
    failures = test_pts.shape[0] - successes
    found_dist = beta(successes + 1, failures + 1)
    found_frac = found_dist.interval(0.68)

    print 'With Color Criteria F160W Mag + Color Cut'
    print 'Samples: {:d} Matched: {:d} Frac: {:0.3g}-{:0.3g}'.format(successes+failures, successes, found_frac[0], found_frac[1])
        
    # dists is by definition the same size as your query points (randx/y)
    # So this makes an array that is True if the random x,y point had a neighbor (dist < infinity), False otherwise.
    has_a_neighbor = dists < np.inf
    # c= sets the color of a point based on the value of the array you give it. 
    # True/False = 1/0 and will result in red and blue points, by default.
    pp.scatter(randx, randy, c=has_a_neighbor, cmap = 'winter')
    pp.show()
    
for folder_name in folder_names: 
    pp.title(folder_name)
    rand_numb(folder_name)



        
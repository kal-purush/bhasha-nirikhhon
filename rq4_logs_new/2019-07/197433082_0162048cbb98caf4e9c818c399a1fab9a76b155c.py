#! /usr/bin/env python

# This script tags locations in the source model/image that are subject to direction-dependent effects.
# mll.sebokolodi@gmail.com or lsebokolodi@ska.ac.za

import matplotlib
matplotlib.use('Agg')

from astropy.io import fits
from astropy.wcs import WCS
import numpy
from astropy.utils.data import get_pkg_data_filename
import os
import Tigger
import argparse



def read_data(image, freq=False):
    """reads image data.

     Set freq=True if a cube. """
                                                                              
    with fits.open(image) as hdu:
        hdr = hdu[0].header
        wcs = WCS(hdr).celestial
        data = hdu[0].data
    imslice = numpy.zeros(data.ndim, dtype=int).tolist()
    imslice[-1] = slice(None)
    imslice[-2] = slice(None)
    if freq:
        imslice[-3] = slice(None)
        
    return data[imslice], hdr, wcs


def negative_noise_estimates(data):

    """ Computes negative data """
    data = data[data < 0]
    negative_data = numpy.concatenate((data, -data))

    return negative_data.std()


def select_bright_locations(data, noise, wcs, thresh_pix=5):

    """ Select regions in an image with intensity > thresh_pix * noise. """

    threshold = thresh_pix * noise
    r, d = numpy.where(data > threshold)
    ra, dec = wcs.all_pix2world(d, r, 0)
     
    return ra, dec

    
def group_sources(ra, dec, data, wcs, tolerance):

    """ Groups the sources within < tolerance """


    pos_world_peak = []
    pos_pixel_peak = []
    peak_flux = []

    def dist(x1, y1, x2, y2):
        return pow(pow(x2 - x1, 2) + pow(y2 - y1, 2), 0.5)

    for i, (r, d) in enumerate(zip(ra, dec)):

        distance  = dist(ra, dec, r, d)
        index = numpy.where(distance < tolerance)
        pos_world = ra[index], dec[index]
        pos_pixel = wcs.all_world2pix(pos_world[0], pos_world[1], 0)

        flux = [data[int(y), int(x)] for x, y in zip(pos_pixel[0], pos_pixel[1])]
        peak_index = numpy.where(flux == max(flux))[0][0]
        
        pos_pixels  = [int(pos_pixel[0][peak_index]), int(pos_pixel[1][peak_index])]
        if pos_pixels not in pos_pixel_peak: 
            pos_world_peak.append([float(pos_world[0][peak_index]), float(pos_world[1][peak_index])])
            pos_pixel_peak.append(pos_pixels)
            peak_flux.append(flux[peak_index])
           
    ra = numpy.array([r[0] for r in pos_world_peak])   
    dec = numpy.array([d[1] for d in pos_world_peak])

    return ra, dec, numpy.asarray(peak_flux)



def compute_local_variance(ra, dec, peak, data, wcs, noise, thresh, 
             region_in_psfs, psf_pix, mask_peak=True):
    
    """ Computes local variance around the source of interest."""

    nx, ny = data.shape
    pos_ra, pos_dec , flux = [], [], []
    size = region_in_psfs * psf_pix # in pixels

    for i, (r, d) in enumerate(zip(ra, dec)):
        x, y = wcs.all_world2pix(r, d, 0)
        x, y = int(x), int(y)

        if (y + size < ny) or (y - size > 0):
            if (x + size < nx) or (x - size > 0):
                if mask_peak:
                    data[y-psf_pix: y + psf_pix, x-psf_pix: x+psf_pix] = float(numpy.nan)
                    sub_region = data[y-size: y+size, x-size: x+size]
                else:
                    sub_region = data[y-size: y+size, x-size: x+size]
                sub_region = sub_region.flatten()
                sub_region = sub_region[~numpy.isnan(sub_region)]
                local_variance = sub_region.std()

        else:
            local_variance = 0
         
        if abs(local_variance) > thresh * noise:
            pos_ra.append(r)
            pos_dec.append(d) 
            flux.append(peak[i])
  
    return numpy.asarray(pos_ra), numpy.asarray(pos_dec), numpy.asarray(flux)



def compute_correlation(ra, dec, peak, data, psfimage, psf_pix, 
         region_in_psfs=5, thresh=0.6):

    """ Correlates a subregion in an image with the PSF """

    size = region_in_psfs * psf_pix # in pixels
    psfdata, psfhdr, pwcs = read_data(psfimage)
    center = int(psfhdr['CRPIX2'])
    
    psf_region = psfdata[center-size:center+size, center-size:center+size]
    psf_subregion = psf_region.flatten()

    nx, ny = data.shape
    pos_ra, pos_dec , flux = [], [], []
    
    for i, (r, d) in enumerate(zip(ra, dec)):
        x, y = wcs.all_world2pix(r, d, 0)
        x, y = int(x), int(y)

        if (y + size < ny) or (y - size > 0):
            if (x + size < nx) or (x - size > 0):
                data_region = data[y-size : y+size, x-size:x+size].flatten()
                normalized_data = (data_region-data_region.min()) / \
                                  (data_region.max()-data_region.min())
                correlate = numpy.corrcoef((normalized_data, psf_subregion))
                correlation_factor =  (numpy.diag((numpy.rot90(correlate))**2).sum())**0.5/2**0.5
        else:
            correlation_factor = 0

        if correlation_factor > thresh:
            pos_ra.append(r)
            pos_dec.append(d)
            flux.append(peak[i])
    return numpy.asarray(pos_ra), numpy.asarray(pos_dec), numpy.asarray(flux)



if __name__=='__main__':

    parser = argparse.ArgumentParser(description= 'Finds the direction subject '
	      'to direction-dependent effects.')
    add = parser.add_argument
    add('-i', '--img', dest='image', help='Image of interest. Required.')
    add('-p', '--psf', dest='psf_image', help='The psf image. Default=None.', default=None)
    add('-c', '--cat', dest='catalog', help='Sky model as LSM/txt. Default=None. Must be in Tigger '
            'format: "#format:name, ra_d, dec_d, i"', default=None)

    add('-fth', '--flux-thresh', dest='flux_threshold', help='Flux threshold. Regions '
        'in an image with flux > fth * noise are considered. Default=5',
         default=5, type=float)

    add('-vth', '--variance-thresh', dest='variance_threshold', help='Local variance threshold. ' 
        'Sources with varinace > vth * noise are considered. Defautl=5.', default=5, type=float)
    add('-vsize', '--var-size', dest='variance_size', help='The size of the region to compute ' 
        ' the local variance. E.g vsize=10, gives a region of size = 10 * resolution.'
        ' The resolution is in pixels. Default=10', 
        default=10, type=int)

    add('-cth', '--correlation-thresh', dest='correlation_threshold', help='Correlation threshold. ' 
        'Sources with correlation factor > cth are considered. Default=0.5', default=0.5, type=float)
    add('-csize', '--corr-size', dest='correlation_size', help='The size of the region to compute ' 
        ' correlation. see vsize. Default=5', default=5, type=int)

    add('-gpix', '--group-pix', dest='group_pixels', help='The size of the region to group the pixels, ' 
        ' in terms of psf-size. The psf is in degrees. e.g gpix=20, gives 20xpsf. Default=20', 
          default=20, type=float)

    add('-o', '--outpref', dest='output_prefix', help='The prefix for the output file containing '
        ' directions in RA, DEC both in degrees, and peak flux of the pixels. Default=None',
          default=None, type=str)
    
    args = parser.parse_args()

    image = get_pkg_data_filename(args.image)
    data, hdr, wcs = read_data(image)
    noise = negative_noise_estimates(data) # the noise

    psf = hdr['bmaj'] # in degrees
    pixels = abs(hdr['CDELT2']) # in degrees
    psf_pix = int(round((psf/pixels))) # in pixels

    # select bright regions from an image:
    ra, dec = select_bright_locations(data, noise, wcs, thresh_pix=args.flux_threshold)        
    # group the sources
    ra, dec, peak = group_sources(ra, dec, data, wcs, tolerance= psf * args.group_pixels)        
    # local variance
    ra, dec, peak = compute_local_variance(ra, dec, peak, data, wcs, noise, 
             thresh=args.variance_threshold, region_in_psfs=args.variance_size,
             psf_pix=psf_pix, mask_peak=False)

    # correlation if a psf is provided.
    if args.psf_image:
        ra, dec, peak = compute_correlation(ra, dec, peak, data, args.psf_image, 
             psf_pix, region_in_psfs=args.correlation_size, 
             thresh=args.correlation_threshold)        

    first_line = ['#format:name','ra_d','dec_d','i']

    if args.output_prefix is None:
        prefix = os.path.basename(args.image.split(",")[0]).split(".")[:-1]
        prefix = '.'.join(prefix)

    outfile = args.output_prefix or prefix
    output = open(outfile + '.txt', 'w')

    for line in first_line:
        output.write('%s '%line)

    for i, (r, d, p) in enumerate(zip(ra, dec, peak)):
         output.write('\nS%d %.3f %.3f %.6f\n'%(i, r, d, p))

    output.close()

    if args.catalog:
        # if a catalog is provided, then tag the sources.
        lsm = Tigger.load(args.catalog, verbose=False)
        directions = Tigger.load(outfile + '.txt', verbose=False)
        tolerance = args.group_pixels * numpy.deg2rad(psf)
        for src in lsm.sources:
            rapos = src.pos.ra
            decpos = src.pos.dec
            within = directions.getSourcesNear(rapos, decpos, tolerance)
            ##TODO: it doesn't seem to want to tag!.
            if len(within) > 0:
                 src.setTag('dE', True)
        lsm.save(args.catalog)

   

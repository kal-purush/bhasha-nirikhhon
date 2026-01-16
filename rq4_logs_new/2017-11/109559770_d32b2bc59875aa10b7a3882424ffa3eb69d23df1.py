"""
Masks part of galaxy image contained within some polygon vertices.
Polygon vertices are exported using the Aladin Sky Atlas 'draw' tool.
"""

import numpy as np
from astropy.io import fits
import func

drawing = input("Enter the path to the Aladin 'Drawing.txt' file:\n")
image = input("Enter the path to the galaxy image file:\n")

outmap = image.split(".")[0] + "_mask.fits"  # output filename


with fits.open(image) as hdulist:
    img = hdulist[0].data  # image data


verts = np.genfromtxt(drawing, skip_header=1, usecols=(4,5))  # extracts verts

grid = func.polymask(img, verts)  # masks points inside polygon vertices

fits.writeto(outmap, grid, overwrite=True)
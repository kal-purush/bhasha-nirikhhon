
"""
Created on Sun Nov  5 15:00:17 2023

Code to process fits files.

This code makes use of a directory where only the current objects images are,
placed. The same directory is reused with files placed there manually for,
reduction. 

Images to be reduced are placed in the appropriate file of the directory and,
then only the file name of the directories used below are changed.

The code queries what filters are used so that it only accesses the directories
which actually have files in them to reduce redundant operations and prevent,
errors. 

The code creates the neccessary bias and flat images to reduce from the images.
It then writes the processed image to a new file.

if there is more than one image of the same type the code will align and,
combine them.
"""
import astroalign as aa
import matplotlib.pyplot as plt
from astropy import units as u

import ccdproc

import numpy as np
from astropy.io import fits
from astropy.nddata import CCDData

import glob 

from ccdproc import wcs_project
from reproject import reproject_interp
from astropy.wcs import WCS


# only search files that contain fits images and then only process those files

obname = str(input('Enter the object name: '))

ireq = (input('is I filter required y/n: '))

rreq = (input('is R filter required y/n: '))

vreq = (input('is V filter required y/n: '))

breq = (input('is B filter required y/n: '))
 
hreq = (input('is Ha filter required y/n: '))

sreq = (input('is SII filter required y/n: '))

haswcs = (input('do(es) the image(s) contain WCS information y/n: '))


#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#




# using a 'standard' file system and placing data sets to be worked on in the
# working directory to keep directory names the same

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

# directory names

# location of all other files and program itself in case needed
maindir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram'


# blue filter directory
bldir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/B'

#blue filter flat directory
blfdir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/Bflat'

# visual filter directory
vdir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/V'

# visual filter flat
vfdir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/Vflat' 

# sulphur2 filter directory
sdir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/OHP-2023-10-17'

# Sulphur 2 filter flat
sfdir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/S2flat'

# Hydrogen alpha filter directory
hdir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/H'

# Hydrogen alpha filter flat directory
hfdir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/Hflat'



idir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/i'

ifdir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/iflat'

rdir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/R'

rfdir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/Rflat'

#-----------------------------------------------------------------------------#
#-----------------------------------------------------------------------------#

# now create lists that contain the fits files from each directory
# also pass through CCCDData()

###############################################################################
#bias
# take in files from bias directory

bidir = 'C:/Users/35385/OneDrive/Desktop/4thyearProject/GenericProgram/Bias'
biasNames = sorted(glob.glob(bidir+'/*')) # /* used as manual directory set up
                                          # guarantees only required fits files 
biases = []                               # are present there.

for i in biasNames:
    biases.append(CCDData(fits.getdata(i),unit = 'adu'))
    
#masterbias = ccdproc.combine(biases, bidir+'Masterbias.FITS','median')
masterbias = ccdproc.combine(biases,None,'median') # set output to none   

###############################################################################

def process_fits(imagedirectory,flatsdirectory,objectname,filtername,bias):
    imdir = imagedirectory
    imNames = sorted(glob.glob(imdir+'/*')) # bluenames

    original_headers = []
 
    image = [] # blue
    for i in imNames:
        hdulist = fits.open(i)
        image.append(CCDData(hdulist[0].data,unit = 'adu'))
        original_headers.append(hdulist[0].header)
        hdulist.close()
    
    #print(image[0])

    flatdir = flatsdirectory
    flatNames = sorted(glob.glob(flatdir+'/*')) # blueflatnames

    flats = [] # blueflats

    for i in flatNames:
        flats.append(CCDData(fits.getdata(i),unit = 'adu'))
        
        masterflat = ccdproc.combine(flats,None,'median')  


    im_takebias = []
    for i in range(0,len(image)):
        im_takebias.append(ccdproc.subtract_bias(image[i],masterbias))      



    mflat_takebias = ccdproc.subtract_bias(masterflat,masterbias)  


    flatcorrect = []
    for i in range(0,len(im_takebias)):
        flatcorrect.append(ccdproc.flat_correct(im_takebias[i],mflat_takebias))
        
        
    ###########################################################################    
    # make new copies in case combination fails.
    # processed files are still available for manual combination
    
    """
    newhead = []
    newfits = []
    for i in range(len(flatcorrect)):
        newhead.append(fits.PrimaryHDU(flatcorrect[i],original_headers[i]))
          
    for i in range(len(newhead)):
       temphdul = fits.HDUList([newhead[i]])
       temphdul.writeto('temp_reconstruct ' + str(i) + filtername + '.fits',overwrite = True)
   """    
   ############################################################################    
        
    imstoalign = []
    for i in imNames:
         hdulist = fits.open(i,mode = 'update')
         #hdulist.info()
         hdulist[0].data  = flatcorrect[0]
         imstoalign.append(hdulist[0])
         hdulist.close()   
    
    
    if(len(imstoalign) > 1 & (haswcs == 'y' or haswcs == 'Y')):
        #alignwcs = []
        ref = imstoalign[0]
        reprojected_ims = []
        for i in range(1,len(imstoalign)):
            temp,tempprint = reproject_interp(imstoalign[i],ref.header)
            reprojected_ims.append(temp)
        
        aligned_combined =  ccdproc.combine(reprojected_ims,maindir + objectname + filtername + '.FITS','median') 
        
    elif(len(flatcorrect) > 1 & (haswcs != 'y' or haswcs != 'Y')):
        print('astroalign ', filtername) # attempts with astroalign but does not always succeed in alignment.
        align = []
        ref = flatcorrect[0] # choose the first image as ref to align to
        align.append(ref) #  ref image is already 'aligned to itself'

        for i in range(1,len(flatcorrect)):
            temp, footprint = aa.register(flatcorrect[i],ref)
            temp =  CCDData(temp,unit = 'adu')
            align.append(temp)  
    
        aligned_combined =  ccdproc.combine(align,maindir + objectname + filtername + '.FITS','median')
    else:
        flatcorrect[0].write(objectname + filtername +'.FITS') 
    
###############################################################################
 
    
if(breq == 'y' or breq == 'Y'):
    process_fits(bldir,blfdir,obname,'B',masterbias)    
    
if(vreq == 'y' or vreq == 'Y'):
    process_fits(vdir,vfdir,obname,'V',masterbias)       
    
if(hreq == 'y' or hreq == 'Y'):
    process_fits(hdir,hfdir,obname,'H',masterbias)       
       
if(sreq == 'y' or sreq == 'Y'):
    process_fits(sdir,sfdir,obname,'SII',masterbias)       
    
if(ireq == 'y' or ireq == 'Y'):
    process_fits(idir,ifdir,obname,'I',masterbias)

if(rreq == 'y' or rreq == 'Y'):
    process_fits(rdir,rfdir,obname,'R',masterbias)         
    
    
    
    
    

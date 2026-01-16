import numpy as np
import lsst.sims.skybrightness as sb
import healpy as hp
from lsst.sims.selfcal.analysis.healplots import healbin
from lsst.sims.utils import _altAzPaFromRaDec
from lsst.sims.maf.utils.telescopeInfo import TelescopeInfo



# Let's see what kind of resolution we have with the all-sky camera.

# Load up the telescope properties, has .lat and .lon
telescope = TelescopeInfo('LSST')


airmassLimit = 2.5
filt = 'R'
starBrightLimit = -9.

nsides = [8,16,32,64]
mapFractions = [ [] for nside in nsides]

# find the indices that are below the airmass limit for each nside
hpIndices = []
for nside in nsides:
    indices = np.arange(hp.nside2npix(nside))
    alt, az = hp.pix2ang(nside,indices)
    alt = np.pi/2.-alt
    airmass = 1./np.cos(np.pi/2.-alt)
    good = np.where((airmass >=1) & (airmass <= airmassLimit) )
    hpIndices.append(indices[good])

# Demand this many stars before using the frame in ubercal
nStarLimit = 200

names = ['ra','dec','starID','starMag', 'starMag_err', 'sky', 'filter']
types = [float,float,float, float,float,float,'|S1']
dtypes = zip(names,types)


# get the max dateID
maxID,mjd = sb.allSkyDB(0,'select max(ID) from dates;', dtypes='int')
maxID = np.max(maxID)

# Crop off some of the early junky observations
minMJD = 56900
minID,mjd = sb.allSkyDB(0,'select min(ID) from dates where mjd > %i;' % minMJD, dtypes='int')

altLimit = 10. # Degrees
sunAltLimit = np.radians(-20.)

maxID = 3000
step = 10

for dateID in np.arange(minID.max(),minID.max()+maxID+1, step):
    sqlQ = 'select stars.ra, stars.dec, stars.ID, obs.starMag_inst, obs.starMag_err,obs.sky, obs.filter from obs, stars, dates where obs.starID = stars.ID and obs.dateID = dates.ID and obs.filter = "%s" and obs.dateID = %i and obs.starMag_err != 0 and dates.sunAlt < %f and obs.starMag_inst > %f;' % (filt,dateID,sunAltLimit, starBrightLimit)

    # Note that RA,Dec are in degrees
    data,mjd = sb.allSkyDB(dateID, sqlQ=sqlQ, dtypes=dtypes)
    if data.size > nStarLimit:
        alt,az,pa = _altAzPaFromRaDec(np.radians(data['ra']), np.radians(data['dec']),
                                      telescope.lon, telescope.lat, mjd)
        for i,nside in enumerate(nsides):
            tempMap = healbin(az,alt, az*0+1, nside=nside, reduceFunc=np.size)[hpIndices[i]]
            ratio = np.size(np.where(tempMap > 0)[0])/float(np.size(tempMap))
            mapFractions[i].append(ratio)


for mf,nside in zip(mapFractions,nsides):
    resol = hp.nside2resol(nside, arcmin=True)/60.
    print 'nside = %i, resolution (deg) = %f, fraction of healpixes populated=%f' % (nside, resol,np.median(mf))
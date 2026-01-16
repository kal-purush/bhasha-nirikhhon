# libraries
import datetime
import os
import math

kernels = os.path.expanduser('~/git_repos/SpaceScienceTutorial/_kernels')
import spiceypy as sp
sp.furnsh(kernels + '/lsk/naif0012.tls')
sp.furnsh(kernels + '/spk/de432s.bsp')
sp.furnsh(kernels + '/pck/gm_de431.tpc')

DATE_TODAY = datetime.datetime.today()
DATE_TODAY = DATE_TODAY.strftime('%Y-%m-%dT00:00:00')
ET_TODAY_MIDNIGHT = sp.utc2et(DATE_TODAY)

print('ET today midnight') # what is et exactly??
print(ET_TODAY_MIDNIGHT)

EARTH_STATE_WRT_SUN, EARTH_SUN_LT = sp.spkgeo(targ=399 \
                                                    ,et=ET_TODAY_MIDNIGHT \
                                                    ,ref='ECLIPJ2000' \
                                                    ,obs=10
                                                    )

print("state vector of the earth wrt the sun for \"today\" (midnight): {0}".format(EARTH_STATE_WRT_SUN))


# check AU
EARTH_SUN_DISTANCE = math.sqrt(EARTH_STATE_WRT_SUN[0] ** 2 \
                                + EARTH_STATE_WRT_SUN[1] ** 2 \
                                + EARTH_STATE_WRT_SUN[2] ** 2
                                )

EARTH_SUN_DISTANCE_AU = sp.convrt(EARTH_SUN_DISTANCE, 'km', 'AU')

print('CURRENT distance between Earth and Sun in AU: {0}'.format(EARTH_SUN_DISTANCE_AU))

EARTH_ORB_SPEED_WRT_SUN = math.sqrt(EARTH_STATE_WRT_SUN[3] ** 2 \
                                + EARTH_STATE_WRT_SUN[4] ** 2 \
                                + EARTH_STATE_WRT_SUN[5] ** 2
                                )
print('Current orbital speed of earth and around the sun in km/s: {0}'.format(EARTH_ORB_SPEED_WRT_SUN))

_, GM_SUN = sp.bodvcd(bodyid=10, item='GM', maxn=1)
V_ORB_FUNC = lambda gm, r: math.sqrt(gm/r)
EARTH_ORB_SPEED_WRT_SUN_THEORY = V_ORB_FUNC(GM_SUN[0], EARTH_SUN_DISTANCE)

print('Theoretical orbital speed of earth around the Sun in km/s: {0}'.format(EARTH_ORB_SPEED_WRT_SUN_THEORY))
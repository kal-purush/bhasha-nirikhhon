import Shadow
import numpy as np
import matplotlib.pyplot as plt
import scipy.interpolate as interpolate

x = in_object_1._beam.getshonecol(1)
z = in_object_1._beam.getshonecol(3)
path = in_object_1._beam.getshonecol(13)

Nx = 101
Nz = Nx
x_lin = np.linspace(np.min(x), np.max(x), Nx)
z_lin = np.linspace(np.min(z), np.max(z), Nx)
X, Z = np.meshgrid(x_lin,z_lin)
wfe = interpolate.griddata((x,z),path,(X,Z),method='linear')

import sys
import numpy
sys.path.insert(0,"/Users/awojdyla/python/poppy/")

from poppy import zernike
# commented import astropy and import poppy_core in poppy repo

#4 defocus
mask2 = np.where(numpy.isnan(wfe))
wfe_proj = wfe.copy()
wfe_proj[mask2] = 0

aperture = wfe.copy()*0+1
aperture[mask2] = 0
ZZ = zernike.arbitrary_basis(aperture, nterms=8, rho=None, theta=None, outside=0)

#defocus
Z3 = ZZ[3,:,:]/np.sqrt(np.sum(ZZ[3,:,:]*ZZ[3,:,:]))
Z7 = ZZ[7,:,:]/np.sqrt(np.sum(ZZ[7,:,:]*ZZ[7,:,:]))

proj3 = np.sum(wfe_proj*Z3)
proj7 = np.sum(wfe_proj*Z7)
res = wfe-proj3*Z3 - proj7*Z7
res_proj = res.copy()
res_proj[mask2]=0

print(proj)
print((np.sum(res_proj*Z3)))

plt.imshow(res)
plt.title("OPD-Z3-Z7")
plt.show()
from __future__ import division
import numpy as np
import mi_cython as mc
import os
from scipy import interpolate
save_path = "/scratch/samrat/kernel/greens/"
pi = np.pi
exp = np.exp
sqrt = np.sqrt
nproc=24*10
procid = int(os.environ['PBS_VNODENUM'])

def derivfd(y):
      dy = np.zeros(np.size(y))
      dy = 0.5 * ( np.roll(y,-1) - np.roll(y,1))
      dy[0] = y[1]-y[0]
      dy[np.size(y)-1] = y[np.size(y)-1]-y[np.size(y)-2]      
      return dy
ellmax = 500
numax = 4.5
numin = 1.5
ndiv = 960
sampling = 2
rsun = 6.9598*10**10
diml = rsun
dimc = 10**6
dimrho = 10**(-3)
dimp = 10**9
width = (50. * 10**5)/rsun;
center_src = 1. -(200. * 10**5)/rsun;
center_rcv = 1. +(200. * 10**5)/rsun

nu = np. linspace ( numin, numax, ndiv) * 0.001
dmpnu = np.loadtxt("m585q.4816" ,usecols= [2])*10**(-6) 
fwhm = np.loadtxt("m585q.4816" ,usecols= [4])*10**(-6)
points = zip(dmpnu, fwhm)
points = sorted(points, key=lambda point: point[0])
dmpnu, fwhm = zip(*points)
damping = interpolate.interp1d(dmpnu, fwhm)
nu = (nu + 1j*damping(nu))*(diml/dimc)

nu0 = 3*0.001
nu0 = (nu0 + 1j*damping(nu0))*diml/dimc
sigma = 0.001
sigma = (sigma + 1j*damping(sigma))*diml/dimc
dnu = derivfd(nu)
fnu = (-1)*(nu**2)*exp(-(nu-nu0)**2 / (2*sigma**2))

r, c, rho, p, Gamma_1, T= np.loadtxt("JCD_modelfull", unpack=True)
r = r[::-1][::sampling]
nr = np.size(r)
c = c[::-1][::sampling]
rho = rho[::-1][::sampling] / dimrho
p = p[::-1][::sampling] / dimp
c2 = c**2 / dimc**2

scaling = derivfd(r)
gravity = (-1)*derivfd(p)/scaling/rho
goverc2 = gravity/c2
hrho = derivfd(np.log(rho))/scaling
N2 = (-1)* gravity*( goverc2 + hrho)
goverc2 = goverc2 + hrho*0.5

rho0 = rho
rho = 1.0
src = exp((-1)*(r-center_src)**2 / (2.0*width**2))
rcv = exp((-1)*(r-center_rcv)**2 / (2.0*width**2))
src_2x = np.append(np.zeros(nr), src)
rcv_2x = np.append(np.zeros(nr), rcv)

for omegai in xrange((procid*ndiv//nproc),(procid*ndiv//nproc+8)):
	omega = nu [ omegai] * 2*pi
	rhoomega2 = rho*(omega**2 - N2)
	xisrc, psrc, xisrcatrcv, xisrcatsrc, psrcatrcv, xircv, prcv, xisrc_denkernel ,xisrc_sskernel, psrc_denkernel, psrc_sskernel,xircv_sskernel, prcv_sskernel =  ([] for i in range(13))
	filename = "omega-"+str(omegai)
	for ell in xrange(ellmax+1):
		print ell
		sl2 = ell*(ell+1)*c2/r**2
		oneoverrhoc2 = (sl2 /omega**2 -1)/ (rho*c2)
		
		my_matrix = mc.genmatrix( goverc2, oneoverrhoc2, rhoomega2, r)
		my_matrix = np.delete(my_matrix, 0,0)
		my_matrix = np.delete(my_matrix, (2*nr-2),0)
		my_matrix = np.delete(my_matrix, 0,1)
		my_matrix = np.delete(my_matrix, (2*nr-2),1)
		
		my_matrix_inv = np.linalg.inv(my_matrix)
		
		src_2x = np.append(np.zeros(nr-1), src)
		src_2x = np.delete(src_2x, (2*nr-2))
		rcv_2x = np.append(np.zeros(nr-1), rcv)
		rcv_2x = np.delete(rcv_2x, (2*nr-2))
		
		solsrc = np.dot(my_matrix_inv, src_2x)
		xis , ps = np.split(solsrc,2)
		xis = np.append(0,xis)
		ps = np.append(ps, 0)
		
		xisrc.append( xis/sqrt(rho0))
		psrc.append( ps*sqrt(rho0))
		
		solrcv = np.dot(my_matrix_inv, rcv_2x)
		xir , pr = np.split(solrcv,2)
		
		xir = np.append(0,xir)
		pr = np.append(pr, 0)
		#--------------------------------------------------------------------
		xircv.append( xir/sqrt(rho0))
		prcv.append( pr*sqrt(rho0))
		#---------------------------------------------------------------------
		xisrcatrcv.append(interpolate.interp1d(r, xisrc[ell])(center_rcv))
		xisrcatsrc.append(interpolate.interp1d(r, xisrc[ell])(center_src))
                psrcatrcv.append(interpolate.interp1d(r, psrc[ell])(center_rcv))
                #----------------------------------------------------------------------
		xisrckernel = ((2*pi)**3 *fnu[omegai]*dnu[omegai]) * xisrc[ell]
		xisrc_denkernel.append( xisrckernel * rho0)
		
                
		dxisrc_dr = derivfd( r**2. * xisrckernel )/( r**2 * scaling )
		xisrc_sskernel.append( c2*dxisrc_dr )
		
                psrckernel = ((2*pi)**3 *fnu[omegai]* dnu[omegai]) * psrc[ell]
		psrc_denkernel.append((psrckernel / (r **2 * rho0 ))/ omega**4)
		psrc_sskernel.append((psrckernel * c2 / (r**2 *rho0)) / omega**2)
		#---------------------------------------------------------------------

                xircv_sskernel.append( derivfd( r**2 *xircv[ell] ) / (r**2 * scaling) )  ##
                prcvkernel = ((2*pi)**3 *fnu[omegai]* dnu[omegai] )* prcv[ell]
		prcv_sskernel.append((prcv[ell] / (r**2. *rho0) )/ omega**2)
	

        np.savez(os.path.join(save_path,filename), r=r, xisrc=xisrc,xisrcatrcv =xisrcatrcv, xisrcatsrc=xisrcatsrc, psrc=psrc, psrcatrcv=psrcatrcv, xircv =xircv, prcv=prcv, xisrc_denkernel=xisrc_denkernel,xisrc_sskernel=xisrc_sskernel, psrc_denkernel=psrc_denkernel, xircv_sskernel=xircv_sskernel, psrc_sskernel=psrc_sskernel, prcv_sskernel = prcv_sskernel)
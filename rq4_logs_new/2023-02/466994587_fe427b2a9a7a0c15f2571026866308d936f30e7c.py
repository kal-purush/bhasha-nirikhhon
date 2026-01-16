#import
import numpy as np
import matplotlib.pyplot as plt
#123
#import NEATM
from NEATM2 import *
#constant
Fsun = 1367.5           # sun constant
sigma = 5.67E-8         #stefan-boltzmann constant
h = 6.626007015E-34     #plank constant
#q = 0.29+0.684*0.15    #phase integral = 0.29+0.684*G(=0.15)
epsi = 0.9             #radiance epsilon
kB = 1.38064852E-23     #boltzmann constant  j/k
cl = 299792458.0        #lightspeed m/s
au = 149597870700.0
pi = 3.1415926535
Rsun = 0.00465*au   #Rsun *m
Tsun = 5778         #Tsun  K
Hv = 22.1
Hv = 21.4
G = 0.15
Ndd = 15
lamdai = [3.4,4.6,12,22]
#.load epoch& cal epoch define x...................................................................
name = '2015TF'
name = '1994CB'
lenchan = 20
lenstep = 4000
#df = pd.read_table(f'ws.dat.{name}')
ast = np.loadtxt(f'./datepoch/ws.dat.{name}',skiprows=1,usecols=(5,6,7))
ear = np.loadtxt(f'./datepoch/ws.dat.{name}',skiprows=1,usecols=(8,9,10))
#ast-sun
d_N = np.sqrt(ast[:,0]**2+ast[:,1]**2+ast[:,2]**2)
#print('ast-sun',d_N)
#ast-earth
delta_N = np.sqrt((ast[:,0]-ear[:,0])**2+(ast[:,1]-ear[:,1])**2+(ast[:,2]-ear[:,2])**2)
#print('ast-earth',delta_N)
#earth-sun
dd_N = np.sqrt(ear[:,0]**2+ear[:,1]**2+ear[:,2]**2)
#print('earth-sun',dd_N)
#alpha
alpha_N = (delta_N*delta_N+d_N*d_N-dd_N*dd_N)/(2*delta_N*d_N)
alpha_N = np.arccos(alpha_N)
x = (delta_N,d_N,alpha_N)
#..load obs dat define y..................................................................
time = np.loadtxt(f'./datjd/mba.jd.{name}')
mjdtime = time-2400000.5
obsdat = np.loadtxt(f'./datwise/obsNEW.txt.{name}')
n4 = len(obsdat)//4
w1 = obsdat[:n4]
w2 = obsdat[n4:2*n4]
w3 = obsdat[2*n4:n4*3]
w4 = obsdat[n4*3:]
y = obsdat
#.......yerr..............
errdat = np.loadtxt(f'./daterr/obsNEWerr.txt.{name}')
erry = errdat[:]
#..model.......................................................
def Model_neatm_Ref_jhx(theta,x,lamda):
    '''
    theta = (eta,D,wf)
    x = (astp,obsp)
    x = (3.4x;4.6x;12x;22x)
    '''
    eta,D,wf = theta
    astp,obsp = x
    lamda1,lamda2,lamda3,lamda4 = lamda
    #print('k',astp)
    pv = (1329*pow(10,-Hv/5)/(D*0.001))**2
    q = 0.29+0.684*G
    A = q*pv
    flux = np.zeros(n4*4)
    for i in range(n4):
        fluxi,frLambi,frLommi = get_flux_ref(astp[i],obsp[i],D,lamda1,eta,A,Hv)
        flux[i] = 1.3917*fluxi + 1.0049*(wf*frLambi + frLommi)
        #print(f'{i}:ast{astp[i]} obs{obsp[i]} flux {flux[i]}')
    for i in range(n4,2*n4):
        fluxi,frLambi,frLommi = get_flux_ref(astp[i],obsp[i],D,lamda2,eta,A,Hv)
        flux[i] = 1.1124*fluxi + 1.0193*(wf*frLambi + frLommi)
        #print(f'{i}:ast{astp[i]} obs{obsp[i]} flux {flux[i]}')
    for i in range(2*n4,n4*3):
        fluxi,frLambi,frLommi = get_flux_ref(astp[i],obsp[i],D,lamda3,eta,A,Hv)
        flux[i] = 0.8791*fluxi 
        #print(f'{i}:ast{astp[i]} obs{obsp[i]} flux {flux[i]}')
    for i in range(n4*3,n4*4):
        fluxi,frLambi,frLommi = get_flux_ref(astp[i],obsp[i],D,lamda4,eta,A,Hv)
        flux[i] = 0.9865*fluxi     
       # print(f'{i}:ast{astp[i]} obs{obsp[i]} flux {flux[i]}')
    return flux    
#.....cal flux with ref
#initial
eta_gs = 1.2
D_gs = 150
eta_gss = [0,5]
D_gss = [0,500]
wf_gs = 0.2
wf_gss = [0,0.5]
#MCMC Function
def log_likelihood(theta, x, y, yerr):
    eta,D,wf = theta
    model = Model_neatm_Ref_jhx(theta,x,lamdai)
    #model = eta*x[0]*100 + D
    #sigma2 = yerr**2 + model**2 * np.exp(2 * logf)
    residuals_rv = y-model
    return -0.5*(np.sum((residuals_rv/yerr)**2 + np.log(2.0*np.pi*(yerr)**2)))
def log_prior(theta):
    eta,D,wf = theta
    if eta_gss[0]< eta < eta_gss[1] and D_gss[0] < D < D_gss[1] and wf_gss[0] < wf < wf_gss[1] :
        return 0.0
    return -np.inf
    #return 0
def log_probability(theta, x, y, yerr):
    lp = log_prior(theta)
    if not np.isfinite(lp):
     #   print('hi','-inf')
        return -np.inf
    yy=lp + log_likelihood(theta, x, y, yerr)
    #print('hi',yy)
    return yy
#................calwith ref
xast = np.vstack((ast,ast,ast,ast))
xear  =  np.vstack((ear,ear,ear,ear))
xt = (xast,xear)
#initial para guesses
eta = eta_gs
D = D_gs
wf = wf_gs
theta = [eta, D,wf]
#.........
import emcee
#initialize sampler
ndim, nwalkers = len(theta), lenchan
sampler = emcee.EnsembleSampler(nwalkers, ndim, log_probability, args=(xt, y, erry))
pos = [theta + 1e-6*np.random.randn(ndim) for i in range(nwalkers)]
#run mcmc
sampler.run_mcmc(pos, lenstep, progress=True);
frac = sampler.acceptance_fraction
np.savetxt(f'./figmcmc/samples/frac.txt.{name}',frac)
samples = sampler.get_chain()
labels = ["eta", "D", "wf"]

for i in range(ndim):
    np.savetxt(f'./figmcmc/samples/samples_{i}.txt.{name}',samples[:,:,i])
#--------------------------
flat_samples = sampler.get_chain(discard=600, thin=15, flat=True)
print(flat_samples.shape)
print(f'NAME={name} len = {n4}')
np.savetxt(f'./figmcmc/samples/flatsam.txt.{name}',flat_samples)
fitans = []
for i in range(ndim):
    mcmc = np.percentile(flat_samples[:, i], [16, 50, 84])
    q = np.diff(mcmc)
    fitans.append([mcmc[1],q[0],q[1]])
D_low,D_fit,D_high = np.percentile(flat_samples[:, 1], [16, 50, 84])
pv = (1329*pow(10,-Hv/5)/(D_fit*0.001))**2
eta_low,eta_fit,eta_high = np.percentile(flat_samples[:, 0], [16, 50, 84])
D_low,D_fit,D_high = np.percentile(flat_samples[:, 1], [16, 50, 84])
wf_low,wf_fit,wf_high = np.percentile(flat_samples[:, 2], [16, 50, 84])
print(f'eta = {eta_fit} D ={D_fit} pv = {pv} wf ={wf_fit}')
fitans.append([0,pv,0])
fitans.append([eta_fit,D_fit,wf_fit])
#plt.errorbar(y,y*0.1,fmt=".k", capsize=1.0)
test = [eta_fit, D_fit, wf_fit]
np.savetxt(f'./ansfit/fit.txt.{name}',fitans)
def loss(obs,cal,err):
    l = len(obs)
    return sum(((obs-cal)/err)**2)/l
print('err=1 : LossFunction = ',loss(y,Model_neatm_Ref_jhx(test,xt,lamdai),1))
print('err=erry : LossFunction = ',loss(y,Model_neatm_Ref_jhx(test,xt,lamdai),erry))


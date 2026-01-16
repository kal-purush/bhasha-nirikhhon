
import numpy as np
import h5py
from scipy.interpolate import CubicSpline
import ezgal
import cosmolopy.distance as cd
import util
import emcee
import scipy.optimize as op
import seaborn as sns
import matplotlib.pyplot as plt
plt.switch_backend("Agg")

def vc(z):
    '''
    dv/dz to impose uniformity
    in redshift
    '''
    cosmo = {'omega_M_0':0.3, 'omega_lambda_0':0.7, 'omega_k_0':0.0, 'h':1.0}
 
    return cd.comoving_volume(z,**cosmo)

def luminosity(m,z):
    '''
    L/Lstar of redsequence galaxies
    '''
    #mchar = kids_mstar(z)
    mchar = CubicSpline(ztest, magtest)(z)
    dm = m - mchar
    exparg = 10. ** (-0.4 * dm)

    return exparg
    
def kids_mstar(zs):
    '''
    returns mstar of redsequence galaxies 
    as observed by kids i band
    '''
    #kcorr = model.get_kcorrects(zf=3.0, zs=zs , filters = "kids") #WE SHALL KCORRECT THE MAGS IF NEEDED
    mags = model.get_apparent_mags(zf=zf , filters = "kids" , zs= zs)

    return mags
class calibrate(object):

    def __init__(self, zmin, zmax, dz, Nb, Nm, Nf, 
                 Nhist, Nab, Nchi , lmin, nbar, 
		 z, l, chi, colors, color_errs, mis, ID, 
		 calib):
        
        # selection summary
	self.lmin = lmin
        self.nbar = nbar

        # ID, color, mag of all gals
        self.ID = ID
        self.colors = colors
        self.color_errs = color_errs
        self.mis = mis
	self.ID = ID
        
        #value added properties of all gals
        self.z = z
	self.l = l
	self.chi = chi

        # ID and z of calib gals
        self.calib = calib   # the whole calibration sample consisting of ID, zrm, chi, l, zspec. the same as calib_sample defined below

        # initial estimates of chi, l, z
        ###self.chi_init = chi_init
        ###self.l_init = l_init
        ###self.z_init = z_init

        # sum numbers for making an initial cut
        ###self.chi_abs = chi_abs
        ###self.zmin_abs = zmin_abs
        ###self.zmax_abs = zmax_abs
        ###self. l_abs = l_abs

        self.zmin = zmin #minimum redshift
        self.zmax = zmax #maximum redshift
        self.dz = dz     #width of redshift bins
        self.Nb = Nb
	self.Nm = Nm
	self.Nf = Nf
        
        self.bnod = np.linspace(self.zmin,self.zmax,self.Nb) #spacing of .05
        self.mnod = np.linspace(self.zmin,self.zmax,self.Nm) #spacing of .1
        self.fnod = np.linspace(self.zmin,self.zmax,self.Nf) #spacing of .14
        self.xrefnod = np.linspace(self.zmin,self.zmax,20) #spacing of .05 
        self.bnod = .5*(self.bnod[1:]+self.bnod[:-1])
        self.fnod = .5*(self.fnod[1:]+self.fnod[:-1])
        self.mnod = .5*(self.mnod[1:]+self.mnod[:-1])

        self.Nhist = Nhist  #the number of bins for making hist of data
        self.Nab = Nab    #the number of spline nodes for afterburner
        self.Nchi = Nchi  #the number of spline nodes for parameterizing chimax
        self.hisnod = np.linspace(self.zmin,self.zmax,self.Nhist) #the nods a which we make hist of data 
        self.abnod = np.linspace(self.zmin,self.zmax,self.Nab) # the afterburner nods
        self.chinod = np.linspace(0.1,0.7,self.Nchi) #the chimax nods

        # the inferred c-m relation parameters
        self.theta = np.loadtxt("opt_theta_bfgs_bounded2.txt")
        self.m = self.theta[0:3*(self.Nm-1)].reshape(self.Nm-1,3) #array of m-nodes
        self.b = self.theta[3*(self.Nm-1):3*(self.Nm+self.Nb-2)].reshape(self.Nb-1,3) #array of b-nodes
        self.lnf = self.theta[3*(self.Nm+self.Nb-2):].reshape(self.Nf-1,3) #array of lnf-nodes

        ####################################HACKY ###############################
        self.bnod = self.bnod[:-1]
        self.mnod = self.mnod[:-1]
        self.fnod = self.fnod[:-1]
        self.m = self.m[:-1,:]
        self.b = self.b[:-1,:]
        self.lnf = self.lnf[:-1,:]
        ####################################################################
        red_file = h5py.File("red_cat.hdf5" , 'r')
        red_sample = red_file['red'][:]
        mrefs = red_file['mref'][:]
        red_file.close()
        znods = np.linspace(self.zmin, self.zmax, 36)
	znods = .5*(znods[1:]+znods[:-1])
	#####################################HACKY#############################
        mrefs = mrefs[znods<0.7]
        znods = znods[znods<0.7]
	#######################################################################
        self.xref = CubicSpline(znods, mrefs)(self.xrefnod)
        
	#######################
        self.dz_theta_zero = 0.01
        
        print "CHI TEST" , self.chi3d() , self.chi 
        print "L TEST" , self.l3d() , self.l
        return None

    def interpolate(self , z):
        """
        interpolate a , b , c arrays from 
        nodes to a given z
        """

        bz = CubicSpline(self.bnod , self.b)(z)
        mz = CubicSpline(self.mnod , self.m)(z)
        lnfz = CubicSpline(self.fnod , self.lnf)(z)
        xrefz = CubicSpline(self.xrefnod , self.xref)(z)
        
        return mz, bz, lnfz, xrefz

    def chi3d(self):
        """
	this recalculates chi3d for every galaxy in the survey for which we have a zred 
	regardless of l and chi
        fast calculation of all the chis for a set of zs 
        z = the current estimate of reshifts
        """
        bz = CubicSpline(self.bnod , self.b)(self.z)
	#print "bz" , bz
        mz = CubicSpline(self.mnod , self.m)(self.z)
        lnfz = CubicSpline(self.fnod , self.lnf)(self.z)
        xrefz = CubicSpline(self.xrefnod , self.xref)(self.z)

        Cint = np.zeros((len(self.z), 3, 3))
        Cint[:,0,0] = np.exp(2.* lnfz[:,0])
        Cint[:,1,1] = np.exp(2.* lnfz[:,1])
        Cint[:,2,2] = np.exp(2.* lnfz[:,2])
        Ctot = self.color_errs + Cint
        res = mz * (self.mis - xrefz)[:,None] + bz - self.colors
        #chis = np.einsum('mn,mn->m', res, np.einsum('ijk,ik->ij',np.linalg.inv(Ctot),res))
        chis = np.einsum('mn,mn->m', res, np.linalg.solve(Ctot,res))
      
        return chis
    
    def l3d(self):
        """
	this recalculates l3d for every galaxy in the survey for which we have a zred
        regardless of its chi and l
	fast calculation of all the luminosity ratios for a set of zs 
        z = the current estimate of reshifts
        """
        ls = luminosity(self.mis, self.z)
                 
        return ls
    
    def ab_lnlike(self, dz_theta, mask):
        """
        dz_theta = estimate of dz at ab spline nods
        mask = some mask on chi and l of spec red gals
        """
        x , y = self.calib[mask,1], self.calib[mask,1]-self.calib[mask,-1]
        dz_pred = CubicSpline(self.abnod , dz_theta)(x)
        chisq = np.sum(np.abs(y - dz_pred))
    
        return chisq
     
    def solve_ab_lnlike(self, mask):
        """
	solves ab_lnlike
	dz_theta_zero = initial guess for dz_theta
	mask = some mask on chi and l of spec red gals
        """
        nll = lambda *args: self.ab_lnlike(*args)

        bnds = []
        for i in range(self.Nab): 
            bnds.append((-0.05,0.05))
	result = op.minimize(nll, self.dz_theta_zero*np.ones((self.Nab)), args=(mask),
                     method='SLSQP', bounds = bnds, 
		     options={'disp': False ,'eps' : .0001})
	
	return result["x"]
    
    def afterburner(self):

    	chinods_0 = 2 + np.zeros((self.Nchi))
        chinods_0[::2] -= .5
	chinods_0[-3:] = 2
	chinods = chinods_0
        chimax_calib = CubicSpline(self.chinod ,chinods)(self.calib[:,1])
	mask = (self.calib[:,2] < chimax_calib)&(self.calib[:,3] > self.lmin)&(self.calib[:,1]<self.zmax)&(self.calib[:,1]>self.zmin)
        dz_ab = self.solve_ab_lnlike(mask)
	print "CURRENT ESTIMATE OF AB = " , dz_ab
        self.calib[:,1] = self.calib[:,1] - CubicSpline(self.abnod, dz_ab)(self.calib[:,1]) #calib gals

        plt.figure(figsize=(7,7))
	plt.scatter(self.calib[mask,1] , self.calib[mask,-1] , s = .01)
   	plt.xlabel(r"$z_{\rm red}$" , fontsize = 25)
    	plt.ylabel(r"$z_{\rm spec}$" , fontsize = 25)
    	plt.xlim([0.05,0.75])
    	plt.ylim([0.05,0.75])
        plt.savefig("/home/vakili/public_html/afterburner_lmin"+str(self.lmin)+".png")
	
	self.z = self.z - CubicSpline(self.abnod , dz_ab)(self.z) #all gals
	print self.z
	#self.z = np.abs(self.z)
        self.chi = self.chi3d()
	self.l = self.l3d()
    
    	return None
 
    def chimax_lnlike(self, chimax_theta):
       
        mask = self.l > self.lmin
    	chinods = np.exp(chimax_theta)
    	norm = self.nbar * 360.3 /(41252.96)
    	
	test_z = self.z[(self.z>0.1)&(self.z<0.7)&(self.l>self.lmin)&(self.chi<5.)]
	test_chi = self.chi[(self.z>0.1)&(self.z<0.7)&(self.l>self.lmin)&(self.chi<5.)]
	chi_maxes = CubicSpline(self.chinod ,chinods)(test_z)
    	sample3 = test_z[(test_chi<chi_maxes)]
	hist , edges = np.histogram(sample3, bins = self.Nhist)
    	bins = .5*(edges[1:]+edges[:-1])
    	dbin = edges[1] - edges[0]
    	dvbin = vc(edges[1:]) - vc(edges[:-1])
    	dvbin = dvbin * 360.3 * pfac/ (41252.96)
    	hist = hist / dvbin

    	chisq = np.sum((hist - self.nbar)**2./ (self.nbar* dvbin**-1.))
    
    	return chisq

    def solve_chimax_lnlike(self):
        """
	solves chi_max_lnlike
        """

    	chinods_0 = 2 + np.zeros((self.Nchi))
        chinods_0[::2] -= .5
	chinods_0[-3:] = 2
        print "chinods_0" , chinods_0    	
	nll = lambda *args: self.chimax_lnlike(*args)
        bnds = []
	for h in range(len(chinods_0)):
	    bnds.append((np.log(.5),np.log(3)))
    	result = op.minimize(nll, np.log(chinods_0) , method='SLSQP', bounds = bnds, options={'disp': True ,'eps' : .001})
    	#result = op.minimize(nll, np.log(chinods_0) , method='Nelder-Mead', options={'disp': True ,'maxiter' : 100})
    	print "esimated chis" , np.exp(result["x"])
        
        
        chi_maxes = CubicSpline(self.chinod, np.exp(result["x"]))(self.z)
    	sample3 = self.z[(self.chi<chi_maxes)&(self.l>self.lmin)&(self.z>0.1)&(self.z<0.7)]
    	print "final sample" , sample3.shape
    	plt.figure(figsize=(10,10)) #FIXME : bins should be 20!
    	sns.distplot(sample3, bins = 40, norm_hist = False, kde = False, hist = True ,  kde_kws={"label": r"$\chi_{\rm red}^{2}<2 , \; L/L_{\star}>0.5$"})
    	
	hist, bin_edges = np.histogram(sample3, bins = 40)
    	bins = .5*(bin_edges[1:]+bin_edges[:-1])
    	dvbin = vc(bin_edges[1:]) - vc(bin_edges[:-1])
    	dvbin = dvbin * 360.3 * pfac / (41252.96)
    
   	plt.plot(bins , self.nbar * dvbin, lw = 2, label = "constant comoving number density")

   	plt.xlabel(r"$z_{\rm red}$" , fontsize = 25)
    	plt.ylabel(r"$dN_{\rm red}/dz_{\rm red}$" , fontsize = 25)
    	plt.xlim([0.05,0.75])
    	plt.legend(loc = 'upper left', fontsize = 20)
        plt.savefig("/home/vakili/public_html/distribution_lmin"+str(self.lmin)+".png")

        uber_mask = (self.chi<chi_maxes)&(self.l>self.lmin)&(self.z>0.1)&(self.z<0.7)
        
	nlrg = self.z[uber_mask].shape[0]
       
        print self.z[uber_mask]
        result_file = h5py.File("LRG_lmin_"+str(self.lmin)+"_nbar_"+str(self.nbar)+".h5" , 'w')
        result_file.create_dataset("ID" , (nlrg,  ) , data = self.ID[uber_mask], dtype = 'S25')
        result_file.create_dataset("mi", (nlrg, ) , data = self.mis[uber_mask], dtype = 'f')
        result_file.create_dataset("redshift", (nlrg, ) , data = self.z[uber_mask], dtype = 'f')
        result_file.create_dataset("colors", (nlrg, 3) , data = self.colors[uber_mask], dtype = 'f')
        result_file.create_dataset("color_errs", (nlrg, 3, 3) , data = self.color_errs[uber_mask], dtype = 'f')
        result_file.close()
        
	return None

def dflens():
    fname = 'KiDS_DR3_x_2dFLenS.txt'
    with open(fname) as f:
         lines = f.readlines()
    ID_2df , z_2df , mi_2df = [] , [] , []
    for i in range(1,len(lines)):
        if (lines[i].split()[:200][-26]!='""'):
	   ID_2df.append('KIDS '+lines[i].split()[:200][1].replace('"',''))
           z_2df.append(lines[i].split()[:200][-3])
	   mi_2df.append(float(lines[i].split()[:200][-26]))
    z_2df = np.array(z_2df ,dtype = float)
    ID_2df = np.array(ID_2df)
    mi_2df = np.array(mi_2df, dtype = float)
    
    return ID_2df , z_2df, mi_2df


if __name__ == '__main__':

   ############ THE ENTIRE KIDS WITH COLORS AND MAGS #############
   reduced_kids = h5py.File("reduced_kids.h5" , "r")

   ID = reduced_kids['ID'][:34290660]
   RA  = reduced_kids['RA'][:34290660]
   DEC = reduced_kids['DEC'][:34290660]
   mi = reduced_kids['mi'][:34290660]
   redshift = reduced_kids['redshift'][:34290660]
   colors = reduced_kids['colors'][:34290660]
   color_errs = reduced_kids['color_errs'][:34290660]
   
   ########## THE ENTIRE KIDS WITH UNCALIB Z l CHI #############
   red_sample = h5py.File("red_photometric_sample_v2.h5" , "r")
   red_sample = red_sample["opt"][:34290660]

   ######### KiDS GALAXIES WITH SPECZ FOR CALIBRATION ######## 
   spec_kids = h5py.File("reduced_speccalibkids.h5" , 'r')
   spec_ID = spec_kids["ID"][:]
   spec_z = spec_kids["redshift"][:]
  
   #spec_ID , spec_index = np.unique(spec_kids["ID"][:] , return_index = True)
   #spec_z = spec_kids["redshift"][:][spec_index]
   
   """ now including 2dflens """
   ID_2df , z_2df , mi_2df  = dflens()
   
   df_ID, df_z = ID_2df, z_2df 
   df_N = df_ID.shape[0]
   df_rand = np.random.randint(0,df_N, df_N/2)
   df_ID , df_z = df_ID[df_rand] , df_z[df_rand]

   ######## MATCHED SPECZ & REDZ ARRAY FOR CALIBRATION #######
   
   spec_ID = np.hstack([spec_ID, df_ID])
   spec_z = np.hstack([spec_z, df_z])
   print "before removal", spec_z.shape   

   spec_ID , spec_index = np.unique(spec_ID , return_index = True)
   spec_z = spec_z[spec_index]
   
   print "after removal", spec_z.shape
   

   mask_one = np.where(np.in1d(ID,spec_ID)==True)[0]
   mask_two = np.where((np.in1d(spec_ID,ID[mask_one])==True))[0]
   
   arg_one = np.argsort(ID[mask_one])
   arg_two = np.argsort(spec_ID[mask_two])


   spec_red_sample = red_sample[mask_one][arg_one]
   #mask_chil = (spec_red_sample[:,0]==1)
   spec_specz = spec_z[mask_two][arg_two]
   
   print "spec red sample shape" , spec_red_sample.shape

   print "spec spec z shape" , spec_specz.shape

   calib_sample = np.vstack([spec_red_sample.T , spec_specz]).T
   
   ##print calib_sample #sanity check
   

   ########## INITIALIZATION OF THE EZGAL MODEL ########

   model = ezgal.model("data/bc03_burst_0.1_z_0.02_salp.model")
   zf = 3.0 #HARDCODED
   model.add_filter("data/i.dat" , "kids" , units = "nm")
   model.set_normalization("sloan_i" , 0.2 , 17.85, vega=False, apparent=True)
   cosmo = {'omega_M_0':0.3, 'omega_lambda_0':0.7, 'omega_k_0':0.0, 'h':1.0}
   ztest = np.linspace(0.02, 1.02, 1000)
   magtest = model.get_apparent_mags(zf=zf , filters = "kids" , zs= ztest)
   

   ######## calibration ##########

   N = red_sample.shape[0]
   pfac = N*1.0/colors.shape[0]
   print "shape of pfac" , pfac
   zmin, zmax, dz = 0.1, 0.8, 0.02
   Nb, Nm, Nf = 15, 8, 6
   Nhis, Nab, Nchi = 40, 7, 6 

   lmin, nbar = 0.5, 0.001
   z, l, chi = red_sample[:N,1], red_sample[:N,3], red_sample[:N,2]
   calib = calib_sample
   print "shape of calib" , calib.shape 
   colors = colors[:N]
   color_errs = color_errs[:N]
   mis = mi[:N]
   ID = ID[:N]

   Qmask = (red_sample[:N,0] == 1)&(red_sample[:N,2]>0)&(red_sample[:N,2]<200)
   
   #print "test", luminosity(mis,z)    

   #cal = calibrate(zmin, zmax, dz, Nb, Nm, Nf, Nhis, Nab, Nchi , lmin, nbar, z, l, chi, colors, color_errs, mis, calib)
   #cal = calibrate(zmin, zmax, dz, Nb, Nm, Nf, Nhis, Nab, Nchi , lmin, nbar, z[Qmask], l[Qmask], chi[Qmask], colors[:N][Qmask], color_errs[:N][Qmask], mis[:N][Qmask], ID[:N][Qmask], calib)
   cal = calibrate(zmin, zmax, dz, Nb, Nm, Nf, Nhis, Nab, Nchi , lmin, nbar, z[Qmask], l[Qmask], chi[Qmask], colors[Qmask], color_errs[Qmask], mis[Qmask], ID[Qmask], calib)
   cal.afterburner()
   cal.solve_chimax_lnlike()
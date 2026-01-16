"""
Module with classes to set up the Cloudy model library with input files and 
scripts to read the output.
"""

# Import other SIGAME modules
import sigame.auxil as aux
import sigame.global_results as glo

# Import other modules
import numpy as np
import pandas as pd
import subprocess as sub
import matplotlib.pyplot as plt
import astropy.units as u
import astropy.constants as c
import scipy as scipy
from scipy import integrate
from scipy.interpolate import interp1d
import csv, subprocess
import pdb as pdb
import copy
import pickle
import shutil


#===============================================================================
###  Load parameters ###
#-------------------------------------------------------------------------------

global params
params                      =   aux.load_parameters()

class library:
    """An object referring to one library of Cloudy models at a specific redshift.

    Parameters
    ----------
    GR: object
        Instance of Global Results object, default: None

    Examples
    --------
    >>> import Cloudy_modeling as clo
    >>> library_obj = clo.library(GR)

    """

    def __init__(self, GR=None, **kwargs):

        p = copy.copy(params)
        for key,val in kwargs.items():
            setattr(p,key,val)

        if GR is None:
            # get global results
            GR                  =   glo.global_results()

        if p.verbose: 
            print("Directory for Cloudy input and output files:")
            print(p.d_cloudy)

    def setup_Mdust_input(self,ext='',**kwargs):
        """Create Cloudy input files to study dust mass as function of Z
        """

        p = copy.copy(params)
        for key,val in kwargs.items():
            setattr(p,key,val)

        Zs                      =   p.grid_Z

        # Load abundance - Z scalings
        abun_scalings   =   pd.read_pickle(p.d_table + 'cloudy/abundances/abun_scalings_solar_' + p.z1)
        f               =   interp1d(abun_scalings['Z_bins'].values,abun_scalings['He'].values,fill_value='extrapolate')
        a_He            =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['C'],fill_value='extrapolate')
        a_C             =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['N'],fill_value='extrapolate')
        a_N             =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['O'],fill_value='extrapolate')
        a_O             =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['Ne'],fill_value='extrapolate')
        a_Ne            =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['Mg'],fill_value='extrapolate')
        a_Mg            =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['Si'],fill_value='extrapolate')
        a_Si            =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['S'],fill_value='extrapolate')
        a_S             =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['Ca'],fill_value='extrapolate')
        a_Ca            =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['Fe'],fill_value='extrapolate')
        a_Fe            =   f(Zs)


        # Edit Cloudy input grid files
        i = 0
        for i_Z in range(len(Zs)):
     
            input_template          =   open(p.d_XL_data + 'cloudy/abundances/template_Mdust.in','r')
            input_copy              =   open(p.d_XL_data + 'cloudy/abundances/cell_test_Mdust_%i.in' % (i_Z),'w')
            for line in input_template:
         
                if line.find('<metals>') >= 0:
                    line = line.replace('<metals>', '%s' % Zs[i_Z])
            
                if line.find('<helium>') >= 0:
                    line = line.replace('<helium>', str(a_He[i_Z]))
                if line.find('<carbon>') >= 0:
                    line = line.replace('<carbon>', str(a_C[i_Z]))
                if line.find('<nitrogen>') >= 0:
                    line = line.replace('<nitrogen>', str(a_N[i_Z]))
                if line.find('<oxygen>') >= 0:
                    line = line.replace('<oxygen>', str(a_O[i_Z]))
                if line.find('<neon>') >= 0:
                    line = line.replace('<neon>', str(a_Ne[i_Z]))
                if line.find('<magnesium>') >= 0:
                    line = line.replace('<magnesium>', str(a_Mg[i_Z]))
                if line.find('<silicon>') >= 0:
                 line = line.replace('<silicon>', str(a_Si[i_Z]))
                if line.find('<sulphur>') >= 0:
                    line = line.replace('<sulphur>', str(a_S[i_Z]))
                if line.find('<calcium>') >= 0:
                    line = line.replace('<calcium>', str(a_Ca[i_Z]))
                if line.find('<iron>') >= 0:
                    line = line.replace('<iron>', str(a_Fe[i_Z]))
         
                input_copy.write(line)
            
            input_template.close()
            input_copy.close()
            i += 1
     
        N_grids = i
        print('Total number of models: ',N_grids)

    def read_Mdust(self,ext='',**kwargs):
        """Read Cloudy output files to study dust mass as function of Z
        """

        p = copy.copy(params)
        for key,val in kwargs.items():
            setattr(p,key,val)

        Zs                      =   p.grid_Z

        M_Z = np.zeros(len(Zs))
        M_d = np.zeros(len(Zs))
        for i_Z in range(len(Zs)):
            out_file_name = p.d_table + 'cloudy/abundances/cell_test_Mdust_%i.out' % i_Z
            M_Z[i_Z],M_He = get_metal_mass(out_file_name)
            M_d[i_Z] = get_dust_mass(out_file_name)

        DTMs = M_d / (M_Z+M_d)

        print(Zs)
        print((0.5*M_Z)/(M_d*(1 - 0.5)))


        df = pd.DataFrame({'M_Z':M_Z, 'M_d':M_d, 'DTMs':DTMs})
        df.to_pickle(p.d_table + 'cloudy/abundances/dust_and_metal_mass')

    def setup_grid_input(self,ext=''):
        """ Store grid parameters and create input files for Cloudy runs
        """

        p = copy.copy(params)

        Mdust_df = pd.read_pickle(p.d_table + 'cloudy/abundances/dust_and_metal_mass')

        # SELECT CLOUDY GRID PARAMETERS
        nHs                     =   p.grid_nH
        DTMs                    =   p.grid_DTM
        FUVs                    =   p.grid_FUV
        CRs                     =   np.log10(2e-16) + FUVs  # Indriolo+2007
        Zs                      =   p.grid_Z
        print('nH :',nHs)
        print('FUV :',FUVs)
        print('CRs : ',CRs)
        print('Zs : ',Zs)

        ext = ''

        # Load and put transmitted continuum in separate files and calculate FUV flux
        cont2 = pd.read_table(p.d_cloudy + 'NH/grid_run_ext.cont2',skiprows=1,names=['E','I_trans','coef'])
        E = cont2.E.values
        i_shift = np.array(['########################### GRID_DELIMIT' in _ for _ in E])
        i_delims = np.arange(len(cont2))[i_shift == True]
        N_E = i_delims[0] # First line is commented
        E_eV = (E[i_delims[0]-N_E:i_delims[0]]).astype(float)*u.Ry.to('eV')
        E_Hz = E_eV * u.eV.to(u.J) / c.h.value
        F_FUV_G0 = np.zeros([len(i_delims)])
        F_FUV_G0_wrong = np.zeros([len(i_delims)])
        F_NUV_ergs_cm2_s = np.zeros([len(i_delims)])
        with open(p.d_cloudy + 'NH/grid_run_ext.cont2','r') as f:
            all_lines = f.readlines()

            for i,i_delim in enumerate(i_delims):
         
                # Start writing input radiation field with correct header
                output_template          =   open(p.d_cloudy + 'table_%i.cont2' % i,'w')
                con_header               =   open(p.d_cloudy + 'NH/cont2_header.txt','r')
                for line in con_header.readlines():
                    output_template.write(line)
             
                # Add recorded transmitted continuum
                for i_line in range(i_delim-N_E,i_delim+1):
                    output_template.write(all_lines[i_line])
             
                # Integrated over FUV wavelengths
                I_erg_s_cm2_Hz = cont2.I_trans[i_delim-N_E:i_delim].astype(float)/E_Hz 
                
                F_FUV_ergs_cm2_s_Hz = I_erg_s_cm2_Hz[(E_eV >= 6) & (E_eV < 13.6)]
             
                F_FUV_G0[i] =  scipy.integrate.simps(F_FUV_ergs_cm2_s_Hz,\
                                E_Hz[(E_eV >= 6) & (E_eV < 13.6)]) / 1.6e-3 # Tielens & Hollenbach 1985 value
             
                I_erg_s_cm2 = cont2.I_trans[i_delim-N_E:i_delim].astype(float) 
                F_FUV_G0_wrong[i] =  np.sum(I_erg_s_cm2[(E_eV >= 6) & (E_eV < 13.6)]) / 1.6e-3
         
                # Integrated NUV (11.09 - 13.6 eV)
                F_NUV_ergs_cm2_s_Hz = I_erg_s_cm2_Hz[(E_eV >= 11.09) & (E_eV < 13.6)]
                F_NUV_ergs_cm2_s[i] = scipy.integrate.simps(F_NUV_ergs_cm2_s_Hz,\
                                        E_Hz[(E_eV >= 11.09) & (E_eV < 13.6)])

                output_template.close()

        # Save scalings to get 1 G0
        scale_factor = 1/F_FUV_G0
       
        out = open(p.d_cloudy + 'NH/grid_run_ext.out','r')
        exts = []
        Zs_ext = []
        start = False
        for line in out.readlines():
           if line == ' **************************************************\n': start = True
           if start:
               if 'STOP COLUMN DENSITY ' in line:
                   exts.append(float(line.split('COLUMN DENSITY ')[1].split(' ')[0]))
               if 'METALS' in line:
                   Zs_ext.append(float(line.split(' ')[2]))
           if line == ' Writing input files has been completed.\n':
               break
        exts = np.array(exts)
        Zs_ext = np.array(Zs_ext)
        
        # Pick only indices with constant Z=1 for the tables
        F_FUV_G0 = F_FUV_G0[Zs_ext == 0]
        indices = np.arange(len(exts))
        NH_indices = indices[(exts >= 17) & (Zs_ext == 0)]
        NHs = exts[NH_indices]
        
        print('Total number of models: ',len(NH_indices)*len(FUVs)*len(Zs)*len(DTMs))
        print(len(NH_indices),len(FUVs),len(Zs),len(DTMs))

        # Load abundance - Z scalings
        abun_scalings   =   pd.read_pickle(p.d_table + 'cloudy/abundances/abun_scalings_solar_%s' % p.z1)
        f               =   interp1d(abun_scalings['Z_bins'].values,abun_scalings['He'].values,fill_value='extrapolate')
        a_He            =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['C'],fill_value='extrapolate')
        a_C             =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['N'],fill_value='extrapolate')
        a_N             =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['O'],fill_value='extrapolate')
        a_O             =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['Ne'],fill_value='extrapolate')
        a_Ne            =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['Mg'],fill_value='extrapolate')
        a_Mg            =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['Si'],fill_value='extrapolate')
        a_Si            =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['S'],fill_value='extrapolate')
        a_S             =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['Ca'],fill_value='extrapolate')
        a_Ca            =   f(Zs)
        f               =   interp1d(abun_scalings['Z_bins'],abun_scalings['Fe'],fill_value='extrapolate')
        a_Fe            =   f(Zs)
        
        
        # Edit Cloudy input grid files
        i = 0
        F_NUV_grid_ergs_cm2_s = np.zeros(len(NH_indices)*len(FUVs)*len(Zs)*len(DTMs))
        all_NHs = np.zeros(len(NH_indices)*len(FUVs)*len(Zs)*len(DTMs))
        all_FUVs = np.zeros(len(NH_indices)*len(FUVs)*len(Zs)*len(DTMs))
        all_Zs = np.zeros(len(NH_indices)*len(FUVs)*len(Zs)*len(DTMs))
        all_DTMs = np.zeros(len(NH_indices)*len(FUVs)*len(Zs)*len(DTMs))
        for i_NH in range(len(NH_indices)):
    
            for i_FUV in range(len(FUVs)):
                scale_table = 10.**FUVs[i_FUV]/F_FUV_G0[i_NH] # linear factor

                for i_Z in range(len(Zs)):
         
                    for i_DTM in range(len(DTMs)):
                        # Calculate grains factor x so that DTM value comes out right:
                        grains = ((10.**DTMs[i_DTM]*Mdust_df.M_Z[i_Z])/(Mdust_df.M_d[i_Z]*(1 - 10.**DTMs[i_DTM])))
                 
                        all_NHs[i] = NHs[i_NH]
                        all_FUVs[i] = FUVs[i_FUV]
                        all_Zs[i] = Zs[i_Z]
                        all_DTMs[i] = DTMs[i_DTM]
                        # Save NUV
                        F_NUV_grid_ergs_cm2_s[i]=   F_NUV_ergs_cm2_s[i_NH] * scale_table 

                        input_template          =   open(p.d_cloudy + 'template_grid_run%s.in' % ext,'r')
                        input_copy              =   open(p.d_cloudy + 'grid_run_%i%s.in' % (i,ext),'w')
                        for line in input_template:
                     
                            if line.find('<hden>') >= 0:
                                line = line.replace('<hden>', '%i %i %i' % (np.min(nHs),np.max(nHs),nHs[1]-nHs[0]))
         
                            if line.find('<table>') >= 0:
                                line = line.replace('<table>', 'table_%i.cont2' % (NH_indices[i_NH]))
                 
                            if line.find('<scale>') >= 0:
                                line = line.replace('<scale>', '%.4f' % (np.log10(scale_table)))
                 
                            if line.find('<ISM>') >= 0:
                                line = line.replace('<ISM>', '%s' % FUVs[i_FUV])
                     
                            if line.find('<CR>') >= 0:
                                line = line.replace('<CR>', '%s' % CRs[i_FUV])
                 
                            if line.find('<metals>') >= 0:
                                line = line.replace('<metals>', '%s' % Zs[i_Z])
                    
                            if line.find('<grains>') >= 0:
                                line = line.replace('<grains>', '%s' % grains)
         
                            if line.find('<helium>') >= 0:
                                line = line.replace('<helium>', str(a_He[i_Z]))
                            if line.find('<carbon>') >= 0:
                                line = line.replace('<carbon>', str(a_C[i_Z]))
                            if line.find('<nitrogen>') >= 0:
                                line = line.replace('<nitrogen>', str(a_N[i_Z]))
                            if line.find('<oxygen>') >= 0:
                                line = line.replace('<oxygen>', str(a_O[i_Z]))
                            if line.find('<neon>') >= 0:
                                line = line.replace('<neon>', str(a_Ne[i_Z]))
                            if line.find('<magnesium>') >= 0:
                                line = line.replace('<magnesium>', str(a_Mg[i_Z]))
                            if line.find('<silicon>') >= 0:
                                line = line.replace('<silicon>', str(a_Si[i_Z]))
                            if line.find('<sulphur>') >= 0:
                                line = line.replace('<sulphur>', str(a_S[i_Z]))
                            if line.find('<calcium>') >= 0:
                                line = line.replace('<calcium>', str(a_Ca[i_Z]))
                            if line.find('<iron>') >= 0:
                                line = line.replace('<iron>', str(a_Fe[i_Z]))
                 
                            input_copy.write(line)
                     
                        input_template.close()
                        input_copy.close()
                        i += 1

        N_grids = i
        np.save(p.d_cloudy + 'number_of_grids',N_grids)
        print('Total number of grids: ',N_grids)

        # Save grid parameters for reading later
        grid_params = {'nHs':nHs,'NHs':NHs,'Zs':Zs,'ISMs':FUVs,'DTMs':DTMs,'F_NUV_ergs_cm2_s':F_NUV_grid_ergs_cm2_s}
        f = open(p.d_table + 'cloudy/grid_params','wb')
        pickle.dump(grid_params,f)
        f.close()

        grid_def = pd.DataFrame({'NH':all_NHs,'FUV':all_FUVs,'Z':all_Zs,'DTM':all_DTMs})
        grid_def.to_pickle(p.d_table + 'cloudy/grid_def') 

        # grid_params = {'nHs':nHs,'NHs':NHs,'Zs':Zs,'FUVs':FUVs,'DTMs':DTMs}
        # f = open(p.d_table + 'cloudy/grid_params','wb')
        # pickle.dump(grid_params,f)
        # f.close()

        print('Ready to run Cloudy!')

    def create_job_scripts(self,ext=''):
        """ Create PBS grid jobs
        """

        p = copy.copy(params)

        N_grids = np.load(p.d_cloudy + "number_of_grids.npy")
        print('Number of grids: %i' % N_grids)

        batch_size = 100
     
        number_of_batches = int(np.floor(N_grids)/batch_size)
     
        for j in range(0,number_of_batches):
            input_template          =   open(p.d_cloudy + 'PBS_template.pbs','r')
            input_copy              =   open(p.d_cloudy + 'PBS_script_%i.pbs' % j,'w')
            for line in input_template:
                input_copy.write(line)
         
            for i in range(j*batch_size,(j+1)*batch_size):
                i_max = (j+1)*batch_size-1
                if i < i_max:
                    line = '/home/u17/karenolsen/code/c17.02/source/cloudy.exe grid_run_%i%s.in &\n' % (i,ext)
                if (i == i_max) | (i%10 == 0): 
                    line = '/home/u17/karenolsen/code/c17.02/source/cloudy.exe grid_run_%i%s.in &\n' % (i,ext)
                input_copy.write(line)
                if (i%10 == 0) & (i%100 != 0):
                    input_copy.write('wait\n')
            input_copy.write('wait')
         
            input_template.close()
            input_copy.close()
   
    def submit_jobs(self,ext=''):
        """ Submit PBS scripts
        """

        p = copy.copy(params)

        N_grids = np.load(p.d_cloudy + "number_of_grids.npy")
        print('Number of grids: %i' % N_grids)

        batch_size = 100
     
        number_of_batches = int(np.floor(N_grids)/batch_size)

        for j in range(0,number_of_batches):
            qsub_command = "qsub %sPBS_script_%i.pbs" % (p.d_cloudy,j)
            exit_status = subprocess.call(qsub_command, shell=True)
            if exit_status == 1:  # Check to make sure the job submitted
                    print("Job {0} failed to submit".format(qsub_command))

        print("Done submitting jobs!")

    def combine_output_files(self,ext=''): 
        """ Combine individual cell models into combined grid output files (for grids that didn't finish)
        """

        p = copy.copy(params)

        #os.chdir('/xdisk/behroozi/mig2020/xdisk/karenolsen/cloudy/')

        with open(p.d_table + 'cloudy/grid_params', 'rb') as handle:
            grid_params = pickle.load(handle)
        NHs         =   grid_params['NHs']
        nHs         =   grid_params['nHs']
        Zs          =   grid_params['Zs']
        FUVs        =   grid_params['ISMs']
        DTMs        =   grid_params['DTMs']
        N_grid      =   len(NHs)*len(Zs)*len(FUVs)*len(DTMs) 
        print('%i grids to go through!' % N_grid)
        for i_grid in range(N_grid):

            found = False
            try:
                grid = open(p.d_cloudy + 'grid_run_%i%s.grd' % (i_grid,ext),'r')
                found = True
            except:
                pass
                print('Grid %i did not run!' % i_grid)
         
            if found == True:
                #if os.path.getmtime('grid_run_%i%s.grd' % (i_grid,ext)) > 1603394124.2773433:
                len_grid = len(grid.readlines())
                #print(len_grid)
                if len_grid < len(nHs)+1:
                    print('Missing lines in grid %i, now assembling ... ' % i_grid)
                    grid = open(p.d_cloudy + 'grid_run_%i%s.grd' % (i_grid,ext),'w')
                    head = '#Index	Failure?	Warnings?	Exit code	#rank	#seq	HDEN=%f L	grid parameter string\n'
                    grid.writelines(head) 
                    #grid.close()
         
                    i = 0
                    missing_zones = 0
                    for nH in nHs:
                    
                        if i < 10: gridnum = '00000000%i' % i
                        if i >= 10: gridnum = '0000000%i' % i
                        if i >= 100: gridnum = '000000%i' % i
                        if i >= 1000: gridnum = '00000%i' % i
                    
                        try: 
                            grd1 = open(p.d_cloudy + 'grid%s_grid_run_%i%s.grd' % (gridnum,i_grid,ext),'r')
                            lines = grd1.readlines()
                        except:
                            #print('Did not find grd output for zone model # %i' % i)
                            lines = []
                    
                        if (len(lines) >= 1):
                            grid.writelines(lines[-1])
                        i += 1
                    print('Missing zone models: %i' % missing_zones)
                    grid.close()           
     
                    ## LINE EMISSION
                   
                    lin = open(p.d_cloudy + 'grid_run_%i%s.lin' % (i_grid,ext),'w')
         
                    head = '#lineslist	C  1 609.590m	C  1 370.269m	C  2 157.636m	O  1 63.1679m	O  1 145.495m	O  3 88.3323m	N  2 205.244m	N  2 121.767m	CO   2600.05m	CO   1300.05m	CO   866.727m	CO   650.074m	CO   325.137m	H2   17.0300m	H2   12.2752m	H2   9.66228m	H2   8.02362m	H2   6.90725m	H2   6.10718m	H2   5.50996m	O  4 25.8832m	NE 2 12.8101m	NE 3 15.5509m	S  3 18.7078m	FE 2 25.9811m\n'
                    lin.writelines(head)
                    
                    i = 0
                    for nH in nHs:
                    
                        if i < 10: gridnum = '00000000%i' % i
                        if i >= 10: gridnum = '0000000%i' % i
                        if i >= 100: gridnum = '000000%i' % i
                     
                        try: 
                            lin1 = open(p.d_cloudy + 'grid%s_grid_run_%i%s.lin' % (gridnum,i_grid,ext),'r')
                            lines = lin1.readlines()
                        except:
                            #print('Did not find lin output for zone model # %i' % i)
                            lines = []
                        if len(lines) >= 1:
                            lin.writelines(lines)
                     
                        #if len(lines) == 0:
                            #print('no lines found for zone model %i' % i)
                     
                        i += 1
                     
                    lin.close()
                     
                     
                    ## OVERVIEW OUTPUT
                   
                    ovr = open(p.d_cloudy + 'grid_run_%i%s.ovr' % (i_grid,ext),'w')
                    head = '#depth	Te	Htot	hden	eden	2H_2/H	HI	HII	HeI	HeII	HeIII	CO/C	C1	C2	C3	C4	O1	O2	O3	O4	O5	O6	H2O/O	AV(point)	AV(extend)\n'
                    ovr.writelines(head)
                   
                    i = 0
                    for nH in nHs:
                    
                        if i < 10: gridnum = '00000000%i' % i
                        if i >= 10: gridnum = '0000000%i' % i
                        if i >= 100: gridnum = '000000%i' % i
                     
                        try: 
                            ovr1 = open(p.d_cloudy + 'grid%s_grid_run_%i%s.ovr' % (gridnum,i_grid,ext),'r')
                            lines = ovr1.readlines()
                        except:
                            #print('Did not find # %i' % i)
                            lines = []
                     
                        if len(lines) >= 1:
                            ovr.writelines(lines)
                     
                        if len(lines) == 0:
                            ovr.writelines('########################### GRID_DELIMIT -- grid%s\n' % (gridnum))
                     
                        i += 1
                     
                    ovr.close()
     
                    ## HYDROGEN OUTPUT
             
                    hyd = open(p.d_cloudy + 'grid_run_%i%s.hyd' % (i_grid,ext),'w')
                    head = '#depth	Te	HDEN	EDEN	HI/H	HII/H	H2/H	H2+/H	H3+/H	H-/H\n' 
                    hyd.writelines(head)
                     
                    i = 0
                    for nH in nHs:
                     
                        if i < 10: gridnum = '00000000%i' % i
                        if i >= 10: gridnum = '0000000%i' % i
                        if i >= 100: gridnum = '000000%i' % i
                     
                        try: 
                            hyd1 = open(p.d_cloudy + 'grid%s_grid_run_%i%s.hyd' % (gridnum,i_grid,ext),'r')
                            lines = hyd1.readlines()
                        except:
                            #print('Did not find # %i' % i)
                            lines = []
                     
                        if len(lines) >= 1:
                            hyd.writelines(lines)
                     
                        if len(lines) == 0:
                            hyd.writelines('########################### GRID_DELIMIT -- grid%s\n' % (gridnum))
                     
                        i += 1
                     
                    hyd.close()
                    if missing_zones == 12:
                        os.remove('grid_run_%i%s.grd' % (i_grid,ext))
                        os.remove('grid_run_%i%s.lin' % (i_grid,ext))
                        os.remove('grid_run_%i%s.ovr' % (i_grid,ext))
                        os.remove('grid_run_%i%s.hyd' % (i_grid,ext))
                        print('hey!')
         
             
                    ## TEMPERATURE OUTPUT
                   
                    heat = open(p.d_cloudy + 'grid_run_%i%s.heat' % (i_grid,ext),'w')
                    head = '#depth cm	Temp K	Htot erg/cm3/s	Ctot erg/cm3/s	heat fracs\n'
                    heat.writelines(head)
                    i = 0
                    for nH in nHs:
                     
                        if i < 10: gridnum = '00000000%i' % i
                        if i >= 10: gridnum = '0000000%i' % i
                        if i >= 100: gridnum = '000000%i' % i
                     
                        try: 
                            heat1 = open(p.d_cloudy + 'grid%s_grid_run_%i%s.heat' % (gridnum,i_grid,ext),'r')
                            lines = heat1.readlines()
                        except:
                            #print('Did not find # %i' % i)
                            lines = []
                   
                        if len(lines) >= 1:
                            heat.writelines(lines)
                   
                        if len(lines) == 0:
                            heat.writelines('########################### GRID_DELIMIT -- grid%s\n' % (gridnum))
                   
                        i += 1
                   
                    heat.close()
                    
    def debug_grid(self,ext=''):
        """ Find Cloudy grids that did not run and restart them
        """

        p = copy.copy(params)
        N_grids = np.load(p.d_cloudy + "number_of_grids.npy")
        print('Number of grids: %i' % N_grids)
        
        batch_size = 50
        ext = ''
        
        # which grids didn't finish?
        indices = []
        for i in range(N_grids):
            try:
                lin = open(p.d_cloudy + 'grid_run_%i.lin' % i,'r')
                count = 0
                for line in lin.readlines(): count += 1
                if count == 1: indices += [i]
            except:
                indices += [i]

        print('Missing: ',len(indices))
        N_grids = len(indices)
        number_of_batches = int(np.floor(N_grids)/batch_size)
        s = aegsg

        for j in range(number_of_batches):
            input_template          =   open(p.d_cloudy + 'PBS_template.pbs','r')
            input_copy              =   open(p.d_cloudy + 'PBS_script_%i.pbs' % j,'w')
            for line in input_template:
                input_copy.write(line)
         
            for ii,i in enumerate(indices[j*batch_size:(j+1)*batch_size]):
                line = '/home/u17/karenolsen/code/c17.02/source/cloudy.exe grid_run_%i%s.in &\n' % (i,ext)
                input_copy.write(line)
                if (ii%5 == 0) & (ii%100 != 0):
                    input_copy.write('wait\n')
            input_copy.write('wait')
         
            input_template.close()
            input_copy.close()

        for j in range(number_of_batches):
            qsub_command = "qsub %sPBS_script_%i.pbs" % (p.d_cloudy,j)
            exit_status = subprocess.call(qsub_command, shell=True)
            if exit_status == 1:  # Check to make sure the job submitted
                print("Job {0} failed to submit".format(qsub_command))

        print("Done submitting debug jobs!")

    def read_grids(self):
        """ Read output Cloudy files and store parameters and luminosities in dataframes.
        """

        print('\nNow reading Cloudy grid models')

        p = copy.copy(params)

        translate_Cloudy_lines = {\
        'O  1 145.495m' : '[OI]145',\
        'O  1 63.1679m' : '[OI]63',\
        'O  3 88.3323m' : '[OIII]88',\
        'O  4 25.8832m' : '[OIV]25',\
        'N  2 205.244m' : '[NII]205',\
        'N  2 121.767m' : '[NII]122',\
        'NE 2 12.8101m' : '[NeII]12',\
        'NE 3 15.5509m' : '[NeIII]15',\
        'C  2 157.636m' : '[CII]158',\
        'C  1 609.590m' : '[CI]610',\
        'C  1 370.269m' : '[CI]370',\
        'CO   2600.05m' : 'CO(1-0)',\
        'CO   1300.05m' : 'CO(2-1)',\
        'CO   866.727m' : 'CO(3-2)',\
        'CO   650.074m' : 'CO(4-3)',\
        'CO   325.137m' : 'CO(8-7)',\
        'S  3 18.7078m' : '[SIII]18',\
        'FE 2 25.9811m' : '[FeII]25',\
    	'H2   17.0300m'	: 'H2_S(1)',\
        'H2   12.2752m' : 'H2_S(2)',\
        'H2   9.66228m'	: 'H2_S(3)',\
        'H2   8.02362m' : 'H2_S(4)',\
        'H2   6.90725m' : 'H2_S(5)',\
        'H2   6.10718m' : 'H2_S(6)',\
        'H2   5.50996m' : 'H2_S(7)'}

        # LOAD FULL GRID PARAMETER LISTS
        with open(p.d_table + 'cloudy/grid_params', 'rb') as handle:
            grid_params = pickle.load(handle)
        nHs         =   grid_params['nHs']
        NHs         =   grid_params['NHs']
        Zs          =   grid_params['Zs']
        FUVs        =   grid_params['ISMs']
        DTMs        =   grid_params['DTMs']
        F_NUV       =   grid_params['F_NUV_ergs_cm2_s']
        N_grid      =   len(nHs)*len(NHs)*len(Zs)*len(FUVs)*len(DTMs)

        # MAKE GRID AS DATAFRAME (same order as in create_cloudy_scripts.py)
        grid_table  =   pd.DataFrame(columns=['NH','FUV','nH','Z','DTM','Exit code','zone_cm','f_H2','f_HI','f_HII'])

        # READ HEADERS - OBS : THIS NEEDS AT LEAST ONE COMPLETE GRID OUTPUT FILE!
        i_complete = 1
        cloudy_grd_header = pd.read_csv(p.d_cloudy + 'grid_run_%i.grd' % (i_complete),sep='\t',header=0).keys()
        #cloudy_lin_header = ['#lineslist','C  1 609.590m','C  1 370.269m','C  2 157.636m','O  1 63.1679m','O  1 145.495m','O  3 88.3323m','N  2 205.244m','N  2 121.767m','CO   2600.05m','CO   1300.05m','CO   866.727m','CO   650.074m','CO   325.137m','O  4 25.8832m','NE 2 12.8101m','NE 3 15.5509m','S  3 18.7078m','FE 2 25.9811m']
        cloudy_lin_header = ['#lineslist','C  1 609.590m','C  1 370.269m','C  2 157.636m','O  1 63.1679m','O  1 145.495m','O  3 88.3323m','N  2 205.244m','N  2 121.767m','CO   2600.05m','CO   1300.05m','CO   866.727m','CO   650.074m','CO   325.137m','H2   17.0300m','H2   12.2752m','H2   9.66228m','H2   8.02362m','H2   6.90725m','H2   6.10718m','H2   5.50996m','O  4 25.8832m','NE 2 12.8101m','NE 3 15.5509m','S  3 18.7078m','FE 2 25.9811m']
        print('Check headers')
        print(cloudy_grd_header)
        print(cloudy_lin_header)

        # ADD LINES TO DATAFRAME
        lines = cloudy_lin_header[1::]
        for line in lines:
            grid_table[line] = []
        grid_table = grid_table.rename(columns=translate_Cloudy_lines)
        lines = [translate_Cloudy_lines[line] for line in lines]

        # NOW READ GRID OUTPUT
        i_grid = 0
        for NH in NHs:
            for FUV in FUVs:
                for Z in Zs:
                    for DTM in DTMs:
                        if i_grid%500 == 0: print(i_grid)
                        # CHECK THAT THIS MODEL EVEN RAN
                        try:
                            lin = open(p.d_cloudy + 'grid_run_%i.lin' % (i_grid),'r')
                            count = 0
                            for line in lin.readlines(): count += 1
                            if count == 1:
                                empty_grid = pd.DataFrame(columns=['NH','FUV','nH','Z','DTM','Exit code','zone_cm','ne','f_H2','f_HI','f_HII','F_NUV_ergs_cm2_s']+lines)
                                empty_grid['nH']     =   nHs
                                empty_grid['NH']     =   np.zeros(len(nHs))+NH
                                empty_grid['FUV']    =   np.zeros(len(nHs))+FUV
                                empty_grid['Z']      =   np.zeros(len(nHs))+Z
                                empty_grid['DTM']    =   np.zeros(len(nHs))+DTM
                                empty_grid['F_NUV_ergs_cm2_s']    =   np.zeros(len(nHs))+F_NUV[i_grid]
                                empty_grid['Exit code']    =   ['']*len(nHs)
                                grid_table = grid_table.append(empty_grid).reset_index(drop=True)
                            else:
                                empty_grid = self.read_one_cloudy_grid(i_grid,lines,\
                                             cloudy_grd_header,cloudy_lin_header,nHs,NH,FUV,Z,DTM,F_NUV[i_grid],\
                                             translate_Cloudy_lines)
                                grid_table = grid_table.append(empty_grid)

                                if i_grid == 240: print(empty_grid['[CII]158'])
                                #grid_table = grid_table.append(self.read_one_cloudy_grid(i_grid,lines,\
                                #             cloudy_grd_header,cloudy_lin_header,nHs,NH,FUV,Z,DTM,F_NUV[i_grid],\
                                #             translate_Cloudy_lines))
                        except:
                            print('Found no output for grid %i' % i_grid)
                            empty_grid = pd.DataFrame(columns=['NH','FUV','nH','Z','DTM','Exit code','zone_cm','ne','f_H2','f_HI','f_HII','F_NUV_ergs_cm2_s']+lines)
                            empty_grid['nH']     =   nHs
                            empty_grid['NH']     =   np.zeros(len(nHs))+NH
                            empty_grid['FUV']    =   np.zeros(len(nHs))+FUV
                            empty_grid['Z']      =   np.zeros(len(nHs))+Z
                            empty_grid['DTM']    =   np.zeros(len(nHs))+DTM
                            empty_grid['F_NUV_ergs_cm2_s']    =   np.zeros(len(nHs))+F_NUV[i_grid]
                            empty_grid['Exit code']    =   ['']*len(nHs)
                            grid_table = grid_table.append(empty_grid).reset_index(drop=True)
                        #if i_grid == 697:
                        #    print(empty_grid)
                        #    pdb.set_trace()
                        if (NH==17) & (FUV==-3) & (Z==-2) & (DTM==-2): 
                            print(i_grid)
                            print(empty_grid['[CII]158'])
                            M_fix           =   1e4*u.Msun.to('kg') # 1e4 Msun [kg]
                            M_current       =   10.**empty_grid.nH.values * c.m_p.value * empty_grid.zone_cm.values # kg
                            M_current[M_current == 0] = -1
                            scale           =   M_fix / M_current 
                            print(scale*empty_grid['[CII]158'].values*u.erg.to('J') / c.L_sun.value)
                    
                        i_grid += 1
                        if len(grid_table) != 12*i_grid: pdb.set_trace()
         
        i_missing = grid_table['Exit code'].values
        i_missing = np.where(i_missing == '       failed assert')[0]

        print('%i 1-zone models with failed assert' % (len(i_missing)))
        print('%i 1-zone models recorded' % (len(grid_table)))
        print(grid_table.head())

        new_exit_code = np.zeros(len(grid_table))
        new_exit_code[i_missing] = 1
        grid_table['Exit code'] = new_exit_code

        # SCALE LUMINOSITIES TO THE SAME H MASS?
        M_fix           =   1e4*u.Msun.to('kg') # 1e4 Msun [kg]
        M_current       =   10.**grid_table.nH * c.m_p.value * grid_table.zone_cm # kg
        M_current[M_current == 0] = -1
        scale           =   M_fix / M_current
        for line in lines:
            lum                 =   scale*grid_table[line].astype(float) # scale to 1e4 Msun
            grid_table[line]    =   lum * u.erg.to('J') / c.L_sun.value # ergs/cm^2/s -> Lsun/cm^2

        # Save dataframe
        grid_table = grid_table.fillna(0)
        grid_table.to_pickle(p.d_table + 'cloudy/grid_table_' + p.z1 + p.grid_ext)
        grid_table.to_csv(p.d_table + 'cloudy/grid_table_' + p.z1 + p.grid_ext + '.csv',float_format='%.4e',index=None,\
            sep='\t',encoding='ascii')

    def read_one_cloudy_grid(self,i_grid,lines,cloudy_grd_header,cloudy_lin_header,nHs,NH,FUV,Z,DTM,F_NUV,translate_Cloudy_lines):
        """ Read one Cloudy grid, filling out missing zones with 0s
        """

        p = copy.copy(params)
        # ADD GRID PARAMS 
        cloudy_grd = pd.read_csv(p.d_cloudy + 'grid_run_%i.grd' % (i_grid),\
            sep='\t',names=cloudy_grd_header,comment='#')
        cloudy_grd = cloudy_grd.rename(columns = {'HDEN=%f L':'nH'}).reset_index(drop=True)
        index = cloudy_grd['#Index'].values
        if len(index) != len(nHs):
            print('len(Grid file) = %i != len(Actual grid) = %i' % (len(index),len(nHs)))
            print('Inserting previous/next grid row for those grid points, but as "failed assert"')
            for i in range(len(nHs)):
                if i not in index:
                    try:
                        cloudy_grd = pd.DataFrame(np.insert(cloudy_grd.values, i, \
                                        values=cloudy_grd.iloc[i-1], \
                                        axis=0),columns=cloudy_grd.keys())
                    except:
                        cloudy_grd = pd.DataFrame(np.insert(cloudy_grd.values, i, \
                                        values=cloudy_grd.iloc[i+1], \
                                        axis=0),columns=cloudy_grd.keys())
                    cloudy_grd['Exit code'][i] = '       failed assert'
                    cloudy_grd['nH'][i] = nHs[i]
        # ADD GRID PARAMETERS
        cloudy_grd['NH']     =   np.zeros(len(nHs))+NH
        cloudy_grd['FUV']    =   np.zeros(len(nHs))+FUV
        cloudy_grd['Z']      =   np.zeros(len(nHs))+Z
        cloudy_grd['DTM']    =   np.zeros(len(nHs))+DTM
        cloudy_grd['F_NUV_ergs_cm2_s']    =   np.zeros(len(nHs))+F_NUV
        # CHECK HOW MANY FAILED 
        i_failed = cloudy_grd['Exit code'].values
        i_failed = np.where((i_failed == '       failed assert') | (i_failed == '   early termination'))[0]
        # ADD LINE LUMINOSITIES
        cloudy_lin = pd.read_csv(p.d_cloudy + 'grid_run_%i.lin' % (i_grid),\
            sep='\t',names=cloudy_lin_header,comment='#').reset_index(drop=True)
        cloudy_lin = cloudy_lin.rename(columns=translate_Cloudy_lines)
        for line in lines:
            cloudy_grd[line] = np.zeros(len(cloudy_grd)) # ergs/cm^2/s
        for line in lines:
            i_lin = 0
            for i in range(len(nHs)):
                if i in i_failed:
                    cloudy_grd[line][i] = 0 # ergs/cm^2/s
                else:
                    cloudy_grd[line][i] = cloudy_lin[line][i_lin] # ergs/cm^2/s
                    i_lin += 1
        if i_grid == 240: print(cloudy_lin[line])
        # ADD ZONE THICKNESS
        ovr = pd.read_csv(p.d_cloudy + 'grid_run_%i.ovr' % i_grid,sep='\t',skiprows=1,index_col=False,\
            names=['depth','Te','Htot','hden','eden','2H_2/H','HI','HII','HeI','HeII','HeIII','CO/C','C1','C2','C3','C4','O1','O2','O3','O4','O5','O6','H2O/O','AV(point)','AV(extend)'])
        depth = ovr.depth.values
        i_shift = np.array(['############ GRID_DELIMIT' in _ for _ in depth])
        depth = depth[i_shift == False]
        #i_delims = np.arange(len(ovr))[i_shift == True]
        max_depth = np.zeros(len(nHs))
        j = 0
        for i in range(len(nHs)):
            if i in i_failed:
                max_depth[i] = 0
            else:
                max_depth[i] = depth[j]
                j += 1
        cloudy_grd['zone_cm'] = max_depth # cm 
        # ADD HYDROGEN MASS FRACTIONS
        hyd = pd.read_csv(p.d_cloudy + 'grid_run_%i.hyd' % i_grid,sep='\t',skiprows=1,index_col=False,\
             comment='#',names=['depth','Te','nH','ne','HI/H','HII/H','H2/H','H2+/H','H3+/H','H-/H'])
        # fill missing values with 0
        for i in range(len(nHs)):
            if i in i_failed:
                hyd = pd.DataFrame(np.insert(hyd.values, i, \
                                   values=[0]*len(hyd.keys()), \
                                   axis=0),columns=hyd.keys())
        cloudy_grd['f_H2'] = hyd['H2/H'].values
        cloudy_grd['f_HI'] = hyd['HI/H'].values
        cloudy_grd['f_HII'] = hyd['HII/H'].values
        cloudy_grd['ne'] = hyd['ne'].values

        if i_grid == 240: print(cloudy_grd['[CII]158'])
        # APPEND ALL
        return(cloudy_grd[['NH','FUV','nH','Z','DTM','Exit code','zone_cm','ne','f_H2','f_HI','f_HII','F_NUV_ergs_cm2_s'] + lines])

    def sample_cloudy_table(self):
        """ Sample Cloudy models in terms of mean density and Mach number
        """

        print('\nNow sampling Cloudy grid models into look-up table')

        p = copy.copy(params)

        # SELECT LOOK-UP TABLE PARAMETERS
        #p.N_param           =   20

        # READ CLOUDY FILES
        try:
            model_number_matrix,grid_table = self._restore_grid_table(grid_ext=p.grid_ext)
        except:
            print('No grid with %s extension' % p.grid_ext)
            pass
        if (p.grid_ext == '_ext'): 
            model_number_matrix_or,grid_table_or = self._restore_grid_table(grid_ext='')
        if (p.grid_ext == '_ext2'): 
            # Just load the default Cloudy grid
            model_number_matrix,grid_table = self._restore_grid_table(grid_ext='')
        if (p.grid_ext == '_ext3'): 
            model_number_matrix,grid_table = self._restore_grid_table(grid_ext='')
        grid_table = grid_table.fillna(0)

        logNHs, logFUVs, lognHs, logZs, logDTMs, F_NUV  =   np.unique(grid_table.NH), np.unique(grid_table.FUV), np.unique(grid_table.nH), np.unique(grid_table.Z),np.unique(grid_table.DTM),grid_table.F_NUV_ergs_cm2_s
        print('NHs: ',logNHs)
        print('FUVs: ',logFUVs)
        print('Zs: ',logZs)
        print('DTMs: ',logDTMs)
        print('nHs: ',lognHs)
        lognH_bin_size = (lognHs[1] - lognHs[0])/2.
        logFUV_bin_size = (logFUVs[1] - logFUVs[0])/2.

        # READ FIT PARAMS OF PDF DEPENDENCY ON SFR DENSITY
        fit_params_SFR = np.load(p.d_table+'fragment/M51_200pc_w_sinks_arepoPDF.npy',allow_pickle=True).item()
        fit_params = fit_params_SFR['fit_params']
        fit_lognH_bins = fit_params_SFR['n_vw_bins'] # log
        fit_lognSFR_bins = fit_params_SFR['SFR_bins'] # log
        fit_lognSFR_bins[0] = -3
        fit_lognH_bins_c = fit_lognH_bins[0:-1] + (fit_lognH_bins[1]-fit_lognH_bins[0])/2
        fit_lognSFR_bins_c = fit_lognSFR_bins[0:-1] + (fit_lognSFR_bins[1]-fit_lognSFR_bins[0])/2

        # Density grid points (more than for PDF fitting)
        range_lognH_mean    =   [-4,2] 
        lognH_means         =   np.linspace(range_lognH_mean[0],range_lognH_mean[1],p.N_param)
        # SFR density grid points (same as for PDF fitting)
        lognSFRs            =   fit_lognSFR_bins_c
        print('volume-av nH: ',lognH_means)
        print('volume-av SFR: ',lognSFRs)

        # START TABLE
        lognH_means_table, lognSFRs_table, logZs_table, logFUVs_table, logNHs_table, logDTMs_table = np.meshgrid(lognH_means, lognSFRs, logZs, logFUVs, logNHs, logDTMs)
        lookup_table = pd.DataFrame({'lognHs':lognH_means_table.flatten(),\
            'lognSFRs':lognSFRs_table.flatten(),\
            'logZs':logZs_table.flatten(),\
            'logFUVs':logFUVs_table.flatten(),\
            'logNHs':logNHs_table.flatten(),\
            'logDTMs':logDTMs_table.flatten()})
        N_models            =   len(lookup_table)
        print('%i models' % N_models)

        # ADD LINE LUMINOSITY AND MASS FRACTIONS
        targets = p.lines
        for target in targets:
            lookup_table[target] = np.zeros(N_models)
            lookup_table[target + '_HII'] = np.zeros(N_models)
            lookup_table[target + '_HI'] = np.zeros(N_models)
            lookup_table[target + '_H2'] = np.zeros(N_models)

        # ADD OTHER TARGETS WE WANT TO SAVE
        lookup_table['V'] = np.zeros(N_models)
        lookup_table['mf_1e3'] = np.zeros(N_models)
        lookup_table['mf_1e1'] = np.zeros(N_models)
        lookup_table['mf_H2'] = np.zeros(N_models)
        lookup_table['mf_HII'] = np.zeros(N_models)
        lookup_table['mf_HI'] = np.zeros(N_models)
        lookup_table['ne'] = np.zeros(N_models)
        lookup_table['ne_mw'] = np.zeros(N_models)
        lookup_table['fail'] = np.zeros(N_models)
        lookup_table['F_NUV_ergs_cm2_s'] = np.zeros(N_models)

        # PDF shape
        x                   =   10.**np.linspace(-5,5,500)
        print('Going through look-up table parameters...')

        N_paramPDF = 0
        N_pot = 0
        i = 0
        for lognH_mean in lognH_means: 
            for iSFR in range(len(lognSFRs)): #[-2]):#enumerate(
                lognSFR = lognSFRs[iSFR]
                #lognH_mean,lognSFR = 2,0.5
                #lognH_mean,lognSFR = -1.6,-2.5
                #print(lognH_mean,lognSFR)

                if '_arepoPDF' in p.table_ext:
                    #print('\n')
                    #print(lognH_mean,lognSFR)
                    if (lognSFR > -30): #fit_params_SFR['SFR_bins'][0]) & (lognH_mean > fit_params_SFR['n_vw_bins'][0]):         
                        if (lognH_mean >= fit_lognH_bins[0]):
                            fit_bin_ntot = np.argmin(np.abs(fit_lognH_bins_c - lognH_mean))
                            fit_bin_SFR = np.argmin(np.abs(fit_lognSFR_bins_c - lognSFR))
                            fit_params_1 = fit_params[fit_bin_ntot,fit_bin_SFR,:]

                            N_paramPDF += 1
                            if np.sum(fit_params_1) != 0:                                
                                PDF_integrated = 10.**aux.parametric_PDF(lognHs,\
                                             lognH_mean,\
                                             fit_params_1[1],\
                                             fit_params_1[2])
                                if fit_params_1[2] == -1.5:
                                    PDF_integrated = 10.**aux.parametric_PDF(lognHs,\
                                             fit_params_1[0],\
                                             fit_params_1[1],\
                                             fit_params_1[2])
                                print('fit_params_1: ',fit_params_1)
                            #pdb.set_trace()

                            #if np.sum(fit_params_2) != 0:
                                #lognHs = np.linspace(-4,6,50)
                                #PDF_integrated_2 = 10.**aux.parametric_PDF(lognHs,\
                                #             fit_params_2[0],\
                                #             fit_params_2[1],\
                                #             fit_params_2[2],\
                                #             fit_params_2[3])
                                #print('fit_params_2: ',fit_params_2)
                                #print(PDF_integrated_2)
                                #PDF_integrated          =   PDF_integrated + PDF_integrated_2
                                #PDF_integrated          =   PDF_integrated/np.sum(PDF_integrated)
                           
                        if (lognH_mean < fit_lognH_bins[0]):
                            PDF_integrated = aux.lognormal_PDF(10.**lognHs,10.**lognH_mean,Mach=1)
                        
                        #print(np.sum(PDF_integrated[lognHs >= 3]))

                if (p.table_ext == '_M10'):         
                    # Default: just one lognormal with Mach = 10
                    PDF_integrated = aux.lognormal_PDF(10.**lognHs,10.**lognH_mean,Mach=10)
                #PDF_integrated1 = aux.lognormal_PDF(10.**lognHs,10.**lognH_mean,Mach=10)
                    
                # Cut  PDF to range 1e-4-1e7
                #PDF_integrated = PDF_integrated[(lognHs >= -4) & (lognHs <= 7)]
                PDF_integrated = PDF_integrated / np.sum(PDF_integrated)

                dMs = PDF_integrated * 1e4 # Msun
                mf_1e1 = np.sum(PDF_integrated[lognHs >= 1])
                mf_1e3 = np.sum(PDF_integrated[lognHs >= 3])
                print('PDF: ',PDF_integrated)
                i_grid = 0
                for iNH,logNH in enumerate(logNHs): #enumerate([17]):#
                    for iFUV,logFUV in enumerate(logFUVs): #enumerate([-3]):#
                        for iZ,logZ in enumerate(logZs): #enumerate([-2]):#
                            for iDTM,logDTM in enumerate(logDTMs): #enumerate([-2]):#
                                #iZ = int(np.argwhere(np.unique(logZs) == logZ))
                                #iFUV = int(np.argwhere(np.unique(logFUVs) == logFUV))
                                #iNH = int(np.argwhere(np.unique(logNHs) == logNH))
                                #iDTM = int(np.argwhere(np.unique(logDTMs) == logDTM))
                                model_number = model_number_matrix[iNH,iFUV,iZ,iDTM]
                                grid_table_cut = grid_table.iloc[model_number]
                                #print(model_number)
                                #print('PDF: ',PDF_integrated)
                                #print(grid_table_cut['[CII]158'])
                                for target in targets:

                                    #target = '[CII]158'#'CO(1-0)'
                                    # Convert to mass and volume fraction in each regime, assuming a gas mass of 1e4 Msun
                                    M = 1e4
                                    dVs = dMs/(10**lognHs*p.mH/p.Msun*p.pc2cm**3) # pc^3
     
                                    # Convert mass fractions to volumes and scale up Cloudy results
                                    Cloudy_values = grid_table_cut[target].values
                                    #if target == '[CII]158':
                                    #    print(Cloudy_values)
                                    #    print(p.grid_ext)

                                    #print(target,Cloudy_values)
                                    if p.grid_ext == '_ext': 
                                        Cloudy_values_or = grid_table_or.iloc[model_number][target].values
                                        Cloudy_values[lognHs <= 2] = Cloudy_values_or[lognHs <= 2]
                                    #if p.grid_ext == '_ext2': 
                                    #    Cloudy_values_or = grid_table_or.iloc[model_number][target].values
                                    #    Cloudy_values[lognHs < 3] = Cloudy_values_or[lognHs < 3]
                                    if ('_ext2' in p.grid_ext):
                                        # Use max NH for high nH
                                        iNH_low = iNH + 2
                                        if iNH_low > len(logNHs)-1: iNH_low = len(logNHs) -1
                                        # AND lower iFUV by 2 dex
                                        iFUV_low = int(iFUV - np.round(2 * (1/logFUV_bin_size))) 
                                        if iFUV_low < 0: iFUV_low = 0
                                        model_number = model_number_matrix[iNH,iFUV_low,iZ,iDTM]
                                        grid_table_cut = grid_table.iloc[model_number]
                                        Cloudy_values_dim = grid_table_cut[target].values
                                    #    if np.max(Cloudy_values) > 1e2:
                                    #        print('\n %s' % target)
                                    #        print(iNH,iNH_low)
                                    #        print(iFUV,iFUV_low)
                                    #        print(Cloudy_values)
                                    #        print(Cloudy_values_dim)
                                    #        print(lognHs)
                                    #        Cloudy_values1 = np.copy(Cloudy_values)
                                    #        Cloudy_values1[lognHs > 2] = Cloudy_values_dim[lognHs > 2]
                                    #        print(PDF_integrated)
                                    #        print(np.sum(PDF_integrated * Cloudy_values))
                                    #        print(np.sum(PDF_integrated * Cloudy_values1))
                                    #        pdb.set_trace()
                                        if p.grid_ext == '_ext2': Cloudy_values[lognHs > 2] = Cloudy_values_dim[lognHs > 2]
                                        if p.grid_ext == '_ext3': Cloudy_values[lognHs > 2] = Cloudy_values_dim[lognHs > 2]
                                    #    if p.grid_ext == '_ext5': Cloudy_values[lognHs > 5] = Cloudy_values_dim[lognHs > 5]
                                    # Weigh Cloudy values by PDF
                                    target_value = np.sum(PDF_integrated * Cloudy_values)
                                    if p.grid_ext == '_dim5':
                                        PDF_integrated = PDF_integrated / np.sum(PDF_integrated[lognHs <= 6])
                                        target_value = np.sum(PDF_integrated[lognHs <= 6] * Cloudy_values[lognHs <= 6])
                                    if target_value == 0 : target_value = 1e-30 
                                    lookup_table[target][i] = np.log10(target_value)

                                    for phase in ['HII','HI','H2']:
                                        mass_fractions = grid_table_cut['f_%s' % phase].values
                                        if phase == 'H2': mass_fractions *= 2
                                        lum_fractions = mass_fractions * PDF_integrated * Cloudy_values
                                        lookup_table[target + '_%s' % phase][i] = np.log10(np.sum(lum_fractions))
                                        #if np.sum(lum_fractions) > 0:
                                        #    print(phase,mass_fractions)
                                        #    print( grid_table_cut['f_HII'].values +  grid_table_cut['f_HI'].values + 2* grid_table_cut['f_H2'].values)
                                        #    print(np.sum(lum_fractions))
                                        #    print(target_value)
                                        #    pdb.set_trace()
                                      
                                    #if target == '[CII]158':
                                    #    print(Cloudy_values)
                                    #    print(10.**lookup_table[target][i])
                                    #    a = aseg
                                if (logZ == 0) & (logNH == 19) & (logDTM == -0.2): 
                                    print(logZ,logNH,logDTM)
                                    print(logFUV,lookup_table['[CII]158'][i])
                                    #lookup_table.to_pickle(p.d_table + 'cloudy/test_lookup_table_' + p.z1 + p.grid_ext + p.table_ext)
                                # Save other properties of grid
                                lookup_table['V'][i] = np.sum(dVs)
                                lookup_table['mf_1e3'][i] = mf_1e3
                                lookup_table['mf_1e1'][i] = mf_1e1
                                lookup_table['mf_H2'][i] = np.sum(PDF_integrated*2*grid_table_cut['f_H2'].values)
                                lookup_table['mf_HII'][i] = np.sum(PDF_integrated*grid_table_cut['f_HII'].values)
                                lookup_table['mf_HI'][i] = np.sum(PDF_integrated*grid_table_cut['f_HI'].values)
                                lookup_table['ne'][i] = np.sum(dVs*grid_table_cut['ne'].values)/np.sum(dVs) 
                                lookup_table['ne_mw'][i] = np.sum(dMs*grid_table_cut['ne'].values)/np.sum(dMs) 
                                if 1 in grid_table_cut['Exit code'].values: 
                                    lookup_table['fail'][i] = np.sum(grid_table_cut['Exit code'].values)
                                lookup_table['lognHs'][i] = lognH_mean
                                lookup_table['lognSFRs'][i] = lognSFR
                                lookup_table['logZs'][i] = logZ
                                lookup_table['logFUVs'][i] = logFUV
                                lookup_table['logNHs'][i] = logNH
                                lookup_table['logDTMs'][i] = logDTM
                                lookup_table['F_NUV_ergs_cm2_s'][i] = F_NUV[i_grid]
     
                                i += 1
                                i_grid += 1
                                if i%10000 == 0: 
                                    print(i)
                                #    lookup_table['Index'] = np.arange(N_models)
                                #    lookup_table.to_pickle(p.d_table + 'cloudy/test_lookup_table_' + p.z1 + p.grid_ext + p.table_ext)
                                #    s = aseg

                    # Another test plot
                    #if (lognH_mean > -1) & (lognSFR > -4): 
                    #    plt.plot(lognHs,np.log10(PDF_integrated))
                    #    plt.savefig(p.d_plot + 'PDF_integrated')
                    #    print('yay!')
                    #    plt.show()

        # Test plot
        # import matplotlib.pyplot as plt
        # plt.plot(np.log10(overdensity),np.log10(PDF))
        print('Number of parametric PDF: ',N_paramPDF)
        #pdb.set_trace()

        lookup_table['Index'] = np.arange(N_models)
        #lookup_table = lookup_table[['Index','logNHs','lognHs','logDTMs','lognSFRs','logZs','logFUVs','V','mf_1e3','mf_1e1','ne','ne_mw','mf_H2','mf_HI','mf_HII','F_NUV_ergs_cm2_s'] + targets]
        lookup_table.to_csv(p.d_table + 'cloudy/lookup_table_' + p.z1 + p.grid_ext + p.table_ext + '.csv',float_format='%.2e',index=None,sep='\t',encoding='ascii')
        lookup_table = lookup_table.fillna(0)
        lookup_table.to_pickle(p.d_table + 'cloudy/lookup_table_' + p.z1 + p.grid_ext + p.table_ext)

    def _restore_grid_table(self,grid_ext=''):

        p = copy.copy(params)

        if grid_ext != '': p.grid_ext = grid_ext # make room for combinations of cloudy grids
        grid_table = pd.read_pickle(p.d_table + 'cloudy/grid_table_' + p.z1 + grid_ext)
        grid_table['DTM'] = np.round(grid_table.DTM.values*10.)/10.

        # Make sure that all columns are float!
        for key in grid_table.keys():
            try:
                grid_table[key] = grid_table[key].values.astype(float)
            except:
                print('%s is not float' % key)

        N_NH            =   len(np.unique(grid_table.NH))
        N_FUV           =   len(np.unique(grid_table.FUV))
        N_nH            =   len(np.unique(grid_table.nH))
        N_Z             =   len(np.unique(grid_table.Z))
        N_DTM           =   len(np.unique(grid_table.DTM))
        NH,FUV,Z,DTM,nH = np.unique(grid_table.NH.values),np.unique(grid_table.FUV.values),np.unique(grid_table.Z.values),np.unique(grid_table.DTM.values),np.unique(grid_table.nH.values)
        # Conversion between location in grid space and model number:
        model_number_matrix = np.zeros([N_NH,N_FUV,N_Z,N_DTM,N_nH])
        i = 0
        for i1 in np.arange(N_NH):
            for i2 in np.arange(N_FUV):
                for i3 in np.arange(N_Z):
                    for i4 in np.arange(N_DTM):
                        for i5 in np.arange(N_nH):
                            model_number_matrix[i1,i2,i3,i4,i5] = i
                            #if i == 240*12: print(i,NH[i1],FUV[i2],Z[i3],DTM[i4],nH[i5])
                            i += 1

        return model_number_matrix, grid_table

    def _restore_lookup_table(self):

        p = copy.copy(params)

        lookup_table = pd.read_pickle(p.d_table + 'cloudy/lookup_table_' + p.z1 + p.grid_ext + p.table_ext)
        # lookup_table['F_NUV_ergs_cm2_s'] = np.log10(lookup_table['F_NUV_ergs_cm2_s'].values) 

        return(lookup_table)

    def calculate_DTM(self,out_file_name='look-up-tables/cloudy/abundances/grid_run_test_Z1.out'):

        p = copy.copy(params)

        M_Z,M_He           =   get_metal_mass(out_file_name)
        M_d                =   get_dust_mass(out_file_name)

        print('Dust-to-metals ratio: %.4e' % (M_d/(M_Z + M_d)))
        print('Dust-to-gas ratio: %.4e' % (M_d/(p.elements['H']['mass'] + M_He + M_Z)))
        print('Metal mass / dust mass: %s' % (M_Z / M_d))
        print('Metal mass: %.4e' % (M_Z))
        print('Dust mass: %.4e' % (M_d))
        print('Example: To have a DTM = 0.5, grains factor needs to be a factor x larger/smaller:')
        print('x = %.5f' % ((0.5*M_Z)/(M_d*(1 - 0.5))))

        return(M_d/(M_Z + M_d))

    def _get_Z_DTM_scaling(self):
        GR                      =   glo.global_results()

        x,y = 'Z','DTM'

        p = copy.copy(params)
        for key,val in kwargs.items():
            setattr(p,key,val)

        # SELECT GALAXIES
        rand_gal_index = np.arange(GR.N_gal)
        xs = np.array([])
        ys = np.array([])
        m_tot,m_encomp,m_y0 = 0,0,0
        for gal_index in rand_gal_index:
            gal_ob              =   gal.galaxy(gal_index)
            df                  =   gal_ob.particle_data.get_dataframe('simgas')
            x1                  =   df[x].values
            y1                  =   df[y].values
            x1[x1 <= p.xlim[0]] = p.xlim[0]
            y1[y1 <= p.ylim[0]] = p.ylim[0]
            m_tot               +=   np.sum(df.m.values)
            m_encomp            +=   np.sum(df.m[(x1>=p.xlim[0]) & (y1>=p.ylim[0])].values)
            m_y0                +=   np.sum(df.m[(y1 == 0)].values)
            ys                  =   np.append(ys,y1[(x1>=p.xlim[0]) & (y1>=p.ylim[0])])
            xs                  =   np.append(xs,x1[(x1>=p.xlim[0]) & (y1>=p.ylim[0])])
        print('Min max of %s:' % x)
        print(xs.min(),xs.max())
        print('Min max of %s:' % y)
        print(ys.min(),ys.max())

        binned_x = np.linspace(np.min(np.log10(xs)),np.max(np.log10(xs)),30)
        binned_x_c = binned_x[0:-1] + (binned_x[1]-binned_x[0])/2
        binned_y = binned_x_c*0.
        print(binned_x)
        for i in range(len(binned_x) -1):
            binned_y[i] = np.median(np.log10(ys)[(xs >= 10**binned_x[i]) & (xs <= 10**binned_x[i+1]) & (ys > 2*p.ylim[0])])
        ax.plot(10**binned_x_c,10**binned_y,color='green',lw=4)
        print(binned_y)

        df = pd.DataFrame({'Z':binned_x,'DTM':binned_DTM})
        df.to_pickle(p.d_table + 'DTM/Z_DTM_scaling_%s' % p.sim_name)

def get_metal_mass(out_file_name):
    p = copy.copy(params)
    # all elements tracked in Simba
    # metals             =   ['C ','N ','O ','Ne','Mg','Si','S ','Ca','Fe'] 
    # all elements in Cloudy output
    metals             =   ['Li','Be','B ','C ','N ','O ','F ','Ne','Na','Mg','Al','Si','P ','S ','Cl','Ar',\
                            'K ','Ca','Sc','Ti','V ','Cr','Mn','Fe']
    M_Z                =   0
    count              =   0
    out_file = open(out_file_name,'r')
    for line in out_file:
        for el in metals:
            if el+':' in line:
                try:
                    m = 10.**float(line.split(el+':')[1].split(' ')[0])
                except:
                    m = 10.**float(line.split(el+': ')[1].split(' ')[0])
                M_Z += m*p.elements[el.replace(':','').replace(' ','')]['mass']
                if 'He:' in line:
                    m_He = 10.**float(line.split('He: ')[1].split(' ')[0])*p.elements['He']['mass']
                count += 1
                if count == len(metals): break
        if count == len(metals): break
    out_file.close()

    return(M_Z,m_He)

def get_dust_mass(out_file_name):
    p = copy.copy(params)
    grains             =   ['Carbonaceous: ','Silicate: ','PAH: ']
    M_grains           =   0
    count              =   0
    out_file = open(out_file_name,'r')
    for line in out_file:
        for gr in grains:
            if gr in line:
                m = p.elements['H']['mass'] * 10.**float(line.split(gr)[2].split(' ')[0])
                M_grains += m
                count += 1
        if count == len(grains): break
    out_file.close()

    return(M_grains)
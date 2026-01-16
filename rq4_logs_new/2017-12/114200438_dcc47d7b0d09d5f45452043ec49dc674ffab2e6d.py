from sys import exit
from sys import argv
from pdb import set_trace
import netCDF4 as nc
import mdtraj as md
from simtk.openmm.app import *
from simtk.openmm import *
from simtk.unit import *
import numpy as np
import pandas as pd
import pymbar as mb
from pymbar import timeseries
from collections import OrderedDict
from smarty import *
from openforcefield.typing.engines.smirnoff import *
from openforcefield.utils import get_data_filename, generateTopologyFromOEMol, read_molecules
import openmmtools.integrators as ommtoolsints
import mdtraj as md
from itertools import product
import pickle
#-------------------------------------------------
def read_traj(ncfiles,indkeep=0):
    """
    Take multiple .nc files and read in coordinates in order to re-valuate energies based on parameter changes

    Parameters
    -----------
    ncfiles - a list of trajectories in netcdf format

    Returns
    ----------
    data - all of the data contained in the netcdf file
    xyzn - the coordinates from the netcdf in angstroms
    """

    data = nc.Dataset(ncfiles)
    
    xyz = data.variables['coordinates']
    xyzn = Quantity(xyz[indkeep:-1], angstroms)   
    
    lens = data.variables['cell_lengths']
    lensn = Quantity(lens[indkeep:-1], angstroms)

    angs = data.variables['cell_angles']
    angsn = Quantity(angs[indkeep:-1], degrees)

    return data, xyzn, lensn, angsn
#------------------------------------------------------------------
def read_traj_vac(ncfiles,indkeep=0):

    data = nc.Dataset(ncfiles)

    xyz = data.variables['coordinates']
    xyzn = Quantity(xyz[indkeep:-1], angstroms)

    return data, xyzn
#------------------------------------------------------------------
def get_energy(system, positions, vecs):
    """
    Return the potential energy.
    Parameters
    ----------
    system : simtk.openmm.System
        The system to check
    positions : simtk.unit.Quantity of dimension (natoms,3) with units of length
        The positions to use
    vecs : simtk.unit.Quantity of dimension 3 with unit of length
        Box vectors to use 
    Returns
    ---------
    energy
    """
    
    integrator = ommtoolsints.LangevinIntegrator(293.15 * kelvin, 1./picoseconds, 1.0 * femtoseconds)
    platform = mm.Platform.getPlatformByName('CPU')
    
    context = mm.Context(system, integrator, platform)
    context.setPeriodicBoxVectors(*vecs)
    context.setPositions(positions)
    state = context.getState(getEnergy=True)
    energy = state.getPotentialEnergy() 
    return energy
#------------------------------------------------------------------
def get_energy_vac(system, positions):
    """
    Return the potential energy.
    Parameters
    ----------
    system : simtk.openmm.System
        The system to check
    positions : simtk.unit.Quantity of dimension (natoms,3) with units of length
        The positions to use

    Returns
    ---------
    energy
    """

    integrator = ommtoolsints.LangevinIntegrator(293.15 * kelvin, 1./picoseconds, 1.0 * femtoseconds)
    platform = mm.Platform.getPlatformByName('CPU')
   
    context = mm.Context(system, integrator, platform)
    
    context.setPositions(positions)
    state = context.getState(getEnergy=True)
    energy = state.getPotentialEnergy()
    return energy

#---------------------------------------------------
def new_param_energy(coords, params, topology, vecs, P=1.01, T=293.15):
    """
    Return potential energies associated with specified parameter perturbations.
    Parameters
    ----------
    coords: coordinates from the simulation(s) ran on the given molecule
    params:  arbitrary length dictionary of changes in parameter across arbitrary number of states. Highest level key is the molecule AlkEthOH_ID,
             second level of keys are the new state, the values of each of these subkeys are a arbitrary length list of length 3 lists where the
             length 3 lists contain information on a parameter to change in the form: [SMIRKS, parameter type, parameter value]. I.e. :

             params = {'AlkEthOH_c1143':{'State 1':[['[6X4:1]-[#1:2]','k','620'],['[6X4:1]-[#6X4:2]','length','1.53'],...],'State 2':[...],...}}
    P: Pressure of the system. By default set to 1.01 bar.
    T: Temperature of the system. By default set to 300 K.

    Returns
    -------
    E_kn: a kxN matrix of the dimensional energies associated with the forcfield parameters used as input
    u_kn: a kxN matrix of the dimensionless energies associated with the forcfield parameters used as input
    """

    #-------------------
    # CONSTANTS
    #-------------------
    kB = 0.0083145  #Boltzmann constant (Gas constant) in kJ/(mol*K)
    beta = 1/(kB*T)

    #-------------------
    # PARAMETERS
    #-------------------
    params = params

    # Determine number of states we wish to estimate potential energies for
    mol2files = []
    for i in params:
        mol2files.append('monomers/'+i.rsplit(' ',1)[0]+'.mol2')

    flavor = oechem.OEIFlavor_Generic_Default | oechem.OEIFlavor_MOL2_Default | oechem.OEIFlavor_MOL2_Forcefield
    mols = []
    mol = oechem.OEMol()
    for mol2file in mol2files:
        ifs = oechem.oemolistream(mol2file)
        ifs.SetFlavor( oechem.OEFormat_MOL2, flavor)
        mol = oechem.OEGraphMol()
        while oechem.OEReadMolecule(ifs, mol):
            oechem.OETriposAtomNames(mol)
            mols.append(oechem.OEGraphMol(mol))
    K = len(params['cyclohexane'].keys())
    
    # Load forcefield file
    ffxml = get_data_filename('forcefield/smirnoff99Frosst.ffxml')
    ff = ForceField(ffxml)

    # Generate a topology
    top = topology#generateTopologyFromOEMol(mol)

    #-----------------
    # MAIN
    #-----------------

    # Calculate energies

    E_kn = np.zeros([K,len(coords)],np.float64)
    u_kn = np.zeros([K,len(coords)],np.float64)
    for i,j in enumerate(params):
        AlkEthOH_id = j
        for k,l in enumerate(params[AlkEthOH_id]):
            print("Anotha one")
            for m,n in enumerate(params[AlkEthOH_id][l]):
                newparams = ff.getParameter(smirks=n[0])
                newparams[n[1]]=n[2]
                ff.setParameter(newparams,smirks=n[0])
                system = ff.createSystem(top,mols,nonbondedMethod=PME,nonbondedCutoff=12.*angstroms)
                barostat = MonteCarloBarostat(P*bar, T*kelvin, 1)
                system.addForce(barostat)
            for o,p in enumerate(coords):
                e = get_energy(system,p,vecs[o])
                E_kn[k,o] = e._value
                u_kn[k,o] = e._value*beta
                

    return E_kn,u_kn
#---------------------------------------------------------------------------
def new_param_energy_vac(coords, params, T=293.15):
    """
    Return potential energies associated with specified parameter perturbations.
    Parameters
    ----------
    coords: coordinates from the simulation(s) ran on the given molecule
    params:  arbitrary length dictionary of changes in parameter across arbitrary number of states. Highest level key is the molecule AlkEthOH_ID,
             second level of keys are the new state, the values of each of these subkeys are a arbitrary length list of length 3 lists where the
             length 3 lists contain information on a parameter to change in the form: [SMIRKS, parameter type, parameter value]. I.e. :

             params = {'AlkEthOH_c1143':{'State 1':[['[6X4:1]-[#1:2]','k','620'],['[6X4:1]-[#6X4:2]','length','1.53'],...],'State 2':[...],...}}
    T: Temperature of the system. By default set to 300 K.

    Returns
    -------
    E_kn: a kxN matrix of the dimensional energies associated with the forcfield parameters used as input
    u_kn: a kxN matrix of the dimensionless energies associated with the forcfield parameters used as input
    """

    #-------------------
    # CONSTANTS
    #-------------------
    kB = 0.0083145  #Boltzmann constant (Gas constant) in kJ/(mol*K)
    beta = 1/(kB*T)

    #-------------------
    # PARAMETERS
    #-------------------
    params = params
    
    # Determine number of states we wish to estimate potential energies for
    mols = []
    for i in params:
        mols.append(i)
    mol = 'monomers/'+mols[0]+'.mol2'
    K = len(params[mols[0]].keys())


    #-------------
    # SYSTEM SETUP
    #-------------
    verbose = False # suppress echos from OEtoolkit functions
    ifs = oechem.oemolistream(mol)
    mol = oechem.OEMol()
    # This uses parm@frosst atom types, so make sure to use the forcefield-flavor reader
    flavor = oechem.OEIFlavor_Generic_Default | oechem.OEIFlavor_MOL2_Default | oechem.OEIFlavor_MOL2_Forcefield
    ifs.SetFlavor( oechem.OEFormat_MOL2, flavor)
    oechem.OEReadMolecule(ifs, mol )
    # Perceive tripos types
    oechem.OETriposAtomNames(mol)

    # Load forcefield file
    ffxml = get_data_filename('forcefield/smirnoff99Frosst.ffxml')
    ff = ForceField(ffxml)

    # Generate a topology
    topology = generateTopologyFromOEMol(mol)

    #-----------------
    # MAIN
    #-----------------

    # Calculate energies

    E_kn = np.zeros([K,len(coords)],np.float64)
    u_kn = np.zeros([K,len(coords)],np.float64)
    for i,j in enumerate(params):
        AlkEthOH_id = j
        for k,l in enumerate(params[AlkEthOH_id]):
            print("Anotha one")
            for m,n in enumerate(params[AlkEthOH_id][l]):
                newparams = ff.getParameter(smirks=n[0]) 
                newparams[n[1]]=n[2]
                ff.setParameter(newparams,smirks=n[0])
                system = ff.createSystem(topology, [mol])
            #print(newparams)
            for o,p in enumerate(coords):
                e = get_energy_vac(system,p)
                E_kn[k,o] = e._value
                u_kn[k,o] = e._value*beta


    return E_kn,u_kn

#-------------------------------------------------------------------------
kB = 0.0083145 #Boltzmann constant (kJ/mol/K)
T = 293.15 #Temperature (K)
N_Av = 6.0221409e23 #particles per mole
N_part = 250. #particles of cyclohexane in box

files = ['cyclohexane_250_[#6X4:1]_epsilon0.1094_rmin_half1.9080.nc']
file_strings = [i.rsplit('.',1)[0].split('_',2)[2] for i in files]

file_str = set(file_strings)

file_tups_traj = [['netCDF4Data_cychex_neat/cyclohexane_250_'+i+'_wAllConstraints_lowPMEco_1fs.nc'] for i in file_str]
file_tups_traj_vac = [['netCDF4Data_cychex_neat/cyclohexane_'+i+'_wAllConstraints_vacuum.nc'] for i in file_str]

file_tups_sd = [['StateData_cychex_neat/cyclohexane_250_'+i+'_wAllConstraints_lowPMEco_1fs.csv'] for i in file_str]
file_tups_sd_vac = [['StateData_cychex_neat/cyclohexane_'+i+'_wAllConstraints_vacuum.csv'] for i in file_str]

params = [i.rsplit('.',1)[0].rsplit('_') for i in files]
params = [[i[3][7:],i[5][4:]] for i in params]
MMcyc = 84.164 #g/mol

states_traj = [[] for i in file_tups_traj] 
states_sd = [[] for i in file_tups_sd]
xyz_orig = [[] for i in file_tups_traj]
xyz_orig_vac = [[] for i in file_tups_traj]
vol_orig = [[] for i in file_tups_traj]
ener_orig = [[] for i in file_tups_sd]
ener_orig_vac = [[] for i in file_tups_sd]
vecs_orig = [[] for i in file_tups_sd]

burnin = 2060#1000
burnin_vac = 7490#3750

print( 'Analyzing Cyclohexane neat liquid trajectories')
for j,i in enumerate(file_tups_traj):
    for ii in i:            
        try:
            data, xyz, lens, angs = read_traj(ii,burnin)             
        except IndexError:
            print( "The trajectory had fewer than %s frames") %(burnin)
            continue 
            
        for m,n in zip(lens,angs):  
            
            vecs = md.utils.lengths_and_angles_to_box_vectors(float(m[0]._value),float(m[1]._value),float(m[2]._value),float(n[0]._value),float(n[1]._value),float(n[2]._value))        
            vecs_orig[j].append(vecs*angstroms)
        for pos in xyz:
            xyz_orig[j].append(pos)
    states_traj[j].append(i[0].rsplit('.',1)[0])

for j,i in enumerate(file_tups_traj_vac):
    for ii in i:
        try:
            data_vac, xyz_vac = read_traj_vac(ii,burnin_vac)
        except IndexError:
            print( "The trajectory had fewer than %s frames") %(burnin)
            continue

    for pos in xyz_vac:
        xyz_orig_vac[j].append(pos)

for j,i in enumerate(file_tups_sd):
    try:
        datasets = [pd.read_csv(ii,sep=',')[burnin:-1] for ii in i]
        merged = pd.concat(datasets)
    except IndexError:
        print( "The state data record had fewer than %s frames") %(burnin)
    for e in merged["Potential Energy (kJ/mole)"]:
        ener_orig[j].append(e)

    for dens in merged["Density (g/mL)"]:
        vol_orig[j].append(MMcyc*dens**(-1))

    states_sd[j].append(i[0].rsplit('.',1)[0])

for j,i in enumerate(file_tups_sd_vac):
    try:
        datasets = [pd.read_csv(ii,sep=',')[burnin_vac:-1] for ii in i]
        merged = pd.concat(datasets)
    except IndexError:
        print( "The state data record had fewer than %s frames") %(burnin)
    for e in merged["Potential Energy (kJ/mole)"]:
        ener_orig_vac[j].append(e)


state_coord = params
param_types = ['epsilon','rmin_half']

ener_orig_sub = [[] for i in ener_orig]
vol_orig_sub = [[] for i in vol_orig]
ener_orig_vac_sub = [[] for i in ener_orig_vac]
xyz_orig_sub = [[] for i in xyz_orig]
xyz_orig_vac_sub = [[] for i in xyz_orig_vac]
vecs_orig_sub = [[] for i in vecs_orig]

for ii,value in enumerate(ener_orig):
    ts = [value]
    g = np.zeros(len(ts),np.float64)

    for i,t in enumerate(ts):
        if np.count_nonzero(t)==0:
            g[i] = np.float(1.)
            print( "WARNING FLAG")
        else:
            g[i] = timeseries.statisticalInefficiency(t)

    N_k_sub = np.array([len(timeseries.subsampleCorrelatedData(t,g=b)) for t, b in zip(ts,g)])
    ind = [timeseries.subsampleCorrelatedData(t,g=b) for t,b in zip(ts,g)]
    inds = ind[0]

    print("Sub-sampling")
    ener_sub = [value[j] for j in inds]
    vol_sub = [vol_orig[ii][j] for j in inds]
    xyz_sub = [xyz_orig[ii][j] for j in inds]
    vecs_sub = [vecs_orig[ii][j] for j in inds]

    ener_orig_sub[ii] = ener_sub
    vol_orig_sub[ii] = vol_sub
    xyz_orig_sub[ii] = xyz_sub
    vecs_orig_sub[ii] = vecs_sub

for ii,value in enumerate(ener_orig_vac):
    ts = [value]
    g = np.zeros(len(ts),np.float64)

    for i,t in enumerate(ts):
        if np.count_nonzero(t)==0:
            g[i] = np.float(1.)
            print( "WARNING FLAG")
        else:
            g[i] = timeseries.statisticalInefficiency(t)

    N_k_vac_sub = np.array([len(timeseries.subsampleCorrelatedData(t,g=b)) for t, b in zip(ts,g)])
    ind_vac = [timeseries.subsampleCorrelatedData(t,g=b) for t,b in zip(ts,g)]
    inds_vac = ind_vac[0]

    print("Sub-sampling")
    ener_vac_sub = [value[j] for j in inds_vac]
    xyz_vac_sub = [xyz_orig_vac[ii][j] for j in inds_vac]

    ener_orig_vac_sub[ii] = ener_vac_sub
    xyz_orig_vac_sub[ii] = xyz_vac_sub

######################################################################################### WORKS^^
# Define new parameter states we wish to evaluate energies at

eps_vals = np.linspace(float(argv[1]),float(argv[2]),3)
rmin_vals = np.linspace(float(argv[3]),float(argv[4]),3)
eps_vals = [str(a) for a in eps_vals]
rmin_vals = [str(a) for a in rmin_vals]
new_states = list(product(eps_vals,rmin_vals))

new_states = list(set(new_states))

orig_state = ('0.1094','1.9080')

N_eff_list = []
param_type_list = []
param_val_list = []

state_coords = []
state_coords.append(orig_state)
for i in new_states:
     state_coords.append(i)

filename = 'packmol_boxes/cyclohexane_250.pdb'
pdb = PDBFile(filename)

for ii,value in enumerate(xyz_orig_sub):
    MBAR_moves = state_coords
    print( "Number of MBAR calculations for liquid cyclohexane: %s" %(len(MBAR_moves)))
    print( "starting MBAR calculations")
    D = OrderedDict()
    for i,val in enumerate(MBAR_moves):
        D['State' + ' ' + str(i)] = [["[#6X4:1]",param_types[j],val[j]] for j in range(len(param_types))]#len(state_orig))]
    D_mol = {'cyclohexane' : D} 
        
    # Produce the u_kn matrix for MBAR based on the subsampled configurations
    E_kn, u_kn = new_param_energy(xyz_orig_sub[ii], D_mol, pdb.topology, vecs_orig_sub[ii], T = 293.15)
#    E_kn_292, u_kn_292 = new_param_energy(xyz_sub[ii], D_mol, pdb.topology, vecs_orig_sub[ii], T = 292.15)
#    E_kn_294, u_kn_294 = new_param_energy(xyz_sub[ii], D_mol, pdb.topology, vecs_orig_sub[ii], T = 294.15)
    
    # Alter u_kn by adding reduced pV term and create an H_kn matrix
    betapV = (1./(kB*T))*101000.*np.array(vol_orig_sub)*1.e-6*1.e-3 
    u_kn += betapV
    H_kn = E_kn + kB*T*betapV 

    K,N = np.shape(u_kn)
                    
    N_k = np.zeros(K)
    N_k[0] = N

    #implement bootstrapping to get variance estimates
    N_eff_boots = []
    u_kn_boots = []
    V_boots = []
    dV_boots =[]
    Cp_boots = []
    dCp_boots = []
    nBoots_work = 1000
    for n in range(nBoots_work):
        for k in range(len(N_k_sub)):
            if N_k_sub[k] > 0:
                if (n == 0):
                    booti = np.array(range(N_k_sub[k]),int)
                else:
                    booti = np.random.randint(N_k_sub[k], size = N_k_sub[k])
        
        E_kn_boot = H_kn[:,booti]       
        u_kn_boot = u_kn[:,booti]
        u_kn_boots.append(u_kn)
        
        # Initialize MBAR with Newton-Raphson
        # Use Adaptive Method (Both Newton-Raphson and Self-Consistent, testing which is better)
        ########################################################################################  
        if (n==0):
            initial_f_k = None # start from zero 
        else:
            initial_f_k = mbar.f_k # start from the previous final free energies to speed convergence        

        mbar = mb.MBAR(u_kn_boot, N_k, verbose=False, relative_tolerance=1e-12,initial_f_k=initial_f_k)
          
        N_eff = mbar.computeEffectiveSampleNumber(verbose=True)
        
        N_eff_boots.append(N_eff)

        (E_expect, dE_expect) = mbar.computeExpectations(E_kn_boot,state_dependent = True)
        (Vol_expect,dVol_expect) = mbar.computeExpectations(vol_sub,state_dependent = False)
        
        V_boots.append(Vol_expect)
        dV_boots.append(dVol_expect)
              
        E_fluc_input = np.array([(E_kn[i][:] - E_expect[i])**2 for i in range(len(E_expect))])
        (E_fluc_expect, dE_fluc_expect) = mbar.computeExpectations(E_fluc_input,state_dependent = True)
        
        C_p_expect_meth2 = E_fluc_expect/(kB * T**2)
        dC_p_expect_meth2 = dE_fluc_expect/(kB * T**2)

        Cp_boots.append(C_p_expect_meth2)
        dCp_boots.append(dC_p_expect_meth2)
        
    u_kn = u_kn_boots[0]
    N_eff = N_eff_boots[0]
    Vol_expect = V_boots[0]
    dVol_expect = dV_boots[0]
    C_p_expect_meth2 = Cp_boots[0]
    dC_p_expect_meth2 = dCp_boots[0]
 
    Cp_boots_vt = np.vstack(Cp_boots)
    V_boots_vt = np.vstack(V_boots)

    C_p_bootstrap = [np.mean(Cp_boots_vt[:,a]) for a in range(np.shape(Cp_boots_vt)[1])] #Mean of Cp calculated with bootstrapping
    dC_p_bootstrap = [np.std(Cp_boots_vt[:,a]) for a in range(np.shape(Cp_boots_vt)[1])] #Standard error of Cp from bootstrap
    Vol_bootstrap = [np.mean(V_boots_vt[:,a]) for a in range(np.shape(V_boots_vt)[1])] #Mean of Cp calculated with bootstrapping
    dVol_bootstrap = [np.std(V_boots_vt[:,a]) for a in range(np.shape(V_boots_vt)[1])] #Standard error of Cp from bootstrap   


    print( "Number of MBAR calculations for cyclohexane in vacuum: %s" %(len(MBAR_moves)))
    print( "starting MBAR calculations")
    D = OrderedDict()
    for i,val in enumerate(MBAR_moves):
        D['State' + ' ' + str(i)] = [["[#6X4:1]",param_types[j],val[j]] for j in range(len(param_types))]#len(state_orig))]
    D_mol = {'cyclohexane' : D}

    # Produce the u_kn matrix for MBAR based on the subsampled configurations
    E_kn_vac, u_kn_vac = new_param_energy_vac(xyz_orig_vac_sub[ii], D_mol, T = 293.15)
    #E_kn_vac_292, u_kn_vac_292 = new_param_energy_vac(xyz_orig_vac_sub[ii], D_mol, T = 292.15)
    #E_kn_vac_294, u_kn_vac_294 = new_param_energy_vac(xyz_orig_vac_sub[ii], D_mol, T = 294.15)

    K_vac,N_vac = np.shape(u_kn_vac)

    N_k_vac = np.zeros(K_vac)
    N_k_vac[0] = N_vac

    N_eff_vac_boots = []
    u_kn_vac_boots = []
    Cp_vac_boots = []
    dCp_vac_boots = []
    nBoots_work = 1000
    for n in range(nBoots_work):
        for k in range(len(N_k_vac_sub)):
            if N_k_vac_sub[k] > 0:
                if (n == 0):
                    booti = np.array(range(N_k_vac_sub[k]),int)
                else:
                    booti = np.random.randint(N_k_vac_sub[k], size = N_k_vac_sub[k])

        E_kn_vac = E_kn_vac[:,booti]
        u_kn_vac = u_kn_vac[:,booti]
        
        u_kn_vac_boots.append(u_kn_vac) 

        # Initialize MBAR with Newton-Raphson
        # Use Adaptive Method (Both Newton-Raphson and Self-Consistent, testing which is better)
        ########################################################################################
        if (n==0):
            initial_f_k = None # start from zero
        else:
            initial_f_k = mbar_vac.f_k # start from the previous final free energies to speed convergence

        mbar_vac = mb.MBAR(u_kn_vac, N_k_vac, verbose=False, relative_tolerance=1e-12,initial_f_k=initial_f_k)

        N_eff_vac = mbar_vac.computeEffectiveSampleNumber(verbose=True)
        
        N_eff_vac_boots.append(N_eff_vac)        

        (E_vac_expect, dE_vac_expect) = mbar_vac.computeExpectations(E_kn_vac,state_dependent = True)
    
        E_vac_fluc_input = np.array([(E_kn_vac[i][:] - E_vac_expect[i])**2 for i in range(len(E_vac_expect))])
        (E_vac_fluc_expect, dE_vac_fluc_expect) = mbar_vac.computeExpectations(E_vac_fluc_input,state_dependent = True)

        C_p_vac_expect_meth2 = E_vac_fluc_expect/(kB * T**2)
        dC_p_vac_expect_meth2 = dE_vac_fluc_expect/(kB * T**2)
        
        Cp_vac_boots.append(C_p_vac_expect_meth2)
        dCp_vac_boots.append(C_p_vac_expect_meth2)

    u_kn_vac = u_kn_vac_boots[0]
    N_eff_vac = N_eff_vac_boots[0]
    C_p_vac_expect_meth2 = Cp_vac_boots[0]
    dC_p_vac_expect_meth2 = dCp_vac_boots[0]

    Cp_vac_boots_vt = np.vstack(Cp_vac_boots) 

    C_p_vac_bootstrap = [np.mean(Cp_vac_boots_vt[:,a]) for a in range(np.shape(Cp_boots_vt)[1])] #Mean of Cp calculated with bootstrapping
    dC_p_vac_bootstrap = [np.std(Cp_vac_boots_vt[:,a]) for a in range(np.shape(Cp_boots_vt)[1])] #Standard error of Cp from bootstrap

    #######################################################################################
    # Calculate residual heat capacity
    #######################################################################################
    C_p_res_expect = [bulk - gas for bulk,gas in zip(C_p_expect_meth2, C_p_vac_expect_meth2)]
    dC_p_res_expect = [np.sqrt(bulk**2 + gas**2) for bulk,gas in zip(dC_p_expect_meth2, dC_p_vac_expect_meth2)]
    
    C_p_res_bootstrap = [bulk - gas for bulk,gas in zip(C_p_bootstrap, C_p_vac_bootstrap)]
    dC_p_res_bootstrap = [np.sqrt(bulk**2 + gas**2) for bulk,gas in zip(dC_p_bootstrap, dC_p_vac_bootstrap)]

    print(Vol_expect,C_p_res_expect,dVol_expect,dVol_bootstrap,dC_p_res_expect,dC_p_res_bootstrap)

    df = pd.DataFrame(
                          {'param_value': MBAR_moves,
                           'Vol_expect (mL/mol)': Vol_expect,
                           'dVol_expect (mL/mol)': dVol_expect,
                           'C_p_res_expect (J/mol/K)': C_p_res_expect,
                           'dC_p_res_expect (J/mol/K)': dC_p_res_expect,
                           'Vol_bootstrap (mL/mol)': Vol_bootstrap,
                           'dVol_bootstrap (mL/mol)': dVol_bootstrap,
                           'C_p_res_bootstrap (J/mol/K)': C_p_res_bootstrap,
                           'dC_p_res_bootstrap (J/mol/K)': dC_p_res_bootstrap,
                           'N_eff': N_eff
                          })
    df.to_csv('MBAR_estimates_[6X4:1]_eps'+argv[1]+'-'+argv[2]+'_rmin'+argv[3]+'-'+argv[4]+'_baro10step_wAllConstraints_VVVR_1fs.csv',sep=';')
    
    with open('param_states_1fs.pkl', 'wb') as f:
        pickle.dump(MBAR_moves, f)
    with open('u_kn_bulk_1fs.pkl', 'wb') as f:
        pickle.dump(u_kn, f)
    with open('u_kn_vac_1fs.pkl', 'wb') as f:
        pickle.dump(u_kn_vac, f)
"""
files = nc.glob('MBAR_estimates_*_baro10step_wAllConstraints_VVVR_1fs.csv')

eps_values = []
rmin_values = []
Vol_expect = []
dVol_expect = []
C_p_expect = []
dC_p_expect = []
Vol_boot = []
dVol_boot = []
C_p_boot = []
dC_p_boot = []
N_eff = []

for i in files:
    df = pd.read_csv(i,sep=';')
    print(i,df.columns)
    new_cols = ['eps_vals', 'rmin_vals']
    df[new_cols] = df['param_value'].str[1:-1].str.split(',', expand=True).astype(str)
    
    df['eps_vals'] = df.eps_vals.apply(lambda x: x.replace("'",""))
    df['rmin_vals'] = df.rmin_vals.apply(lambda x: x.replace("'",""))
    
    df['eps_vals'] = df.eps_vals.apply(lambda x: float(x))
    df['rmin_vals'] = df.rmin_vals.apply(lambda x: float(x))
 
    eps_temp = df.eps_vals.values.tolist()
    rmin_temp = df.rmin_vals.values.tolist()
    Vol_temp = df['Vol_expect (mL/mol)'].values.tolist()
    dVol_temp = df['dVol_expect (mL/mol)'].values.tolist()
    Cp_temp = df['C_p_res_expect (J/mol/K)'].values.tolist()
    dCp_temp = df['dC_p_res_expect (J/mol/K)'].values.tolist()
    Vol_boot_temp = df['Vol_bootstrap (mL/mol)'].values.tolist()
    dVol_boot_temp = df['dVol_bootstrap (mL/mol)'].values.tolist()
    Cp_boot_temp = df['C_p_res_bootstrap (J/mol/K)'].values.tolist()
    dCp_boot_temp = df['dC_p_res_bootstrap (J/mol/K)'].values.tolist()
    Neff_temp = df.N_eff.values.tolist()

    for i in eps_temp:
        eps_values.append(i)
    for i in rmin_temp:
        rmin_values.append(i)
    for i in Vol_temp:
        Vol_expect.append(i)
    for i in dVol_temp:
        dVol_expect.append(i)
    for i in Cp_temp:
        C_p_expect.append(i)
    for i in dCp_temp:
        dC_p_expect.append(i)
    for i in Vol_boot_temp:
        Vol_boot.append(i)
    for i in dVol_boot_temp:
        dVol_boot.append(i)
    for i in Cp_boot_temp:
        C_p_boot.append(i)
    for i in dCp_boot_temp:
        dC_p_boot.append(i)
    for i in Neff_temp:
        N_eff.append(i)
print(len(eps_values),len(rmin_values),len(C_p_expect),len(dVol_boot),len(N_eff))

df2 = pd.DataFrame(
                  {'epsilon values': eps_values,
                   'rmin_half values': rmin_values,
                   'Vol_expect (mL/mol)': Vol_expect,
                   'dVol_expect (mL/mol)': dVol_expect,
                   'C_p_res_expect (J/mol/K)': C_p_expect,
                   'dC_p_res_expect (J/mol/K)': dC_p_expect,
                   'Vol_bootstrap (mL/mol)': Vol_boot,
                   'dVol_bootstrap (mL/mol)': dVol_boot,
                   'C_p_res_bootstrap (J/mol/K)': C_p_boot,
                   'dC_p_res_bootstrap (J/mol/K)': dC_p_boot,
                   'N_eff': N_eff
                  })

df2 = df2.drop_duplicates()

df2.to_csv('MBAR_estimates_[6X4:1]_eps_0.1022-0.1157_rmin_half_1.8870-1.9260_total_baro10step_wAllConstraints_vvvr_1fs.csv',sep=';')
"""
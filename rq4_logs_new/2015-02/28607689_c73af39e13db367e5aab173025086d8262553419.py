from random import choice, random
import os
import subprocess

def mat13(N, ndenmax=0.08494, ndenp=0.001, xmax=50, xp=0.1, ymax=50, yp=0.1,
zmax=50, zp=0.1, epmax=500.0, epp=0.1, sigmax=8.0, sigp=0.01, qmax=6.0):
    
#max number density based on that of pure Iron
#max unit cell dimensions based on PCN-777 cages size
#max LJ parameters (for now using 1.5x highest values in GenericMOFs)
#max charge... UFF?

    if type(N) != int:
        print 'N must be an integer.'
   
    Ntag = str(N)
    ntag = str(ndenmax)
    xtag = str(xmax)
    ytag = str(xmax)
    ztag = str(xmax)
    eptag = str(xmax)
    sigtag = str(xmax)
    qtag = str(xmax)
    
    top_path = ('materials' + '_' + Ntag + '.' + ntag + '_' + xtag + '.' + ytag
		 + '.' + ztag + '_' + eptag + '.' + sigtag + '_' + qtag)
    
    if not os.path.exists(top_path):
        os.mkdir(top_path) 
    def drange(start, stop, step):
        r = start
        while r < stop:
            yield r
            r+= step
    
    nden0 = drange(1, ndenmax*10000, ndenp*10000)
    ndendim = [nden for nden in nden0]
    
    x0 = drange(0, xmax + xp, xp)
    xdim = [x for x in x0]
    
    y0 = drange(0, ymax + yp, yp)
    ydim = [y for y in y0]
    
    z0 = drange(0, zmax + zp, zp)
    zdim = [z for z in z0]
    
    ep0 = drange(0, epmax + epp, epp)
    epdim = [ep for ep in ep0]
    
    sig0 = drange(0, sigmax + sigp, sigp)
    sigdim = [sig for sig in sig0]
    
    #open mat_stats.txt, to track material data    
    mat_stats = open(os.path.abspath(top_path)+ '/mat_stats.txt', 'w')
    mat_stat_heading = ('\nBOUNDARIES\nNumber of particles: ' + Ntag +
                    	'\nnumber density:   ' + ntag + '\nx-coordinate: ' +
			xtag + '\ny-coordinate: ' + ytag + '\nz-coordinate: ' +
			 ztag + '\nEpsilon: ' + eptag + '\nSigma: ' + sigtag 
			+ '\nCharge: ' + qtag + '\n\n' +
			'#name     number density     xdim     ydim     '+
			'zdim     total particles     net charge\n')
    mat_stats.write(mat_stat_heading)
    
    #MAT-XXX loop...
    for i in range(N + 1):
	
        mat_name = 'MAT-' + str(i)
        
	#make MAT-XXX directory
	os.mkdir(top_path+'/'+mat_name)
	
	#open .cif file
        cif_file = open(os.path.abspath(top_path) + '/'+mat_name + '/' + 
			mat_name+'.cif', 'w')
	
	#open force_field_mixing_rules.def
	mixing_rules = open(os.path.abspath(top_path) + '/'+mat_name +
			'/force_field_mixing_rules.def', 'w')
        
	#open pseudo_atoms.def
        pseudo_atoms = open(os.path.abspath(top_path) + '/'+mat_name + 
			'/pseudo_atoms.def', 'w')
	
	#open force_field.def
        force_field = open(os.path.abspath(top_path) + '/'+mat_name +
			'/force_field.def', 'w')

 	nden_ = choice(ndendim)/10000.
        xdim_ = choice(xdim)
        ydim_ = choice(ydim)
        zdim_ = choice(zdim)
	N_ = xdim_ * ydim_ * zdim_ * nden_
	n_ = int(N_)        

        cif_heading = ('material' + str(i) + 
			'\n\nloop_\n_symmetry_equiv_pos_as_xyz\n  x,y,z\n'+
			'_cell_length_a          ' + str(xdim_) +
			'\n_cell_length_b          ' + str(ydim_) +
			'\n_cell_length_c          ' + str(zdim_) + 
			'\n_cell_angle_alpha       90.0000\n' +
			'_cell_angle_beta        90.0000\n' +
			'_cell_angle_gamma       90.0000\nloop_\n' +
			'_atom_site_label\n_atom_site_fract_x\n' +
			'_atom_site_fract_y\n_atom_site_fract_z\n'+
			'_atom_site_charge\n')
	cif_file.write(cif_heading)

        mixing_heading = ('# general rule for shifted vs truncated\nshifted\n' +
			'# general rule for tailcorrections\nno\n' +
			'# number of defined interactions\n' + str(108) +  #check these + XXX values
			'\n# type interaction\n')
        mixing_rules.write(mixing_heading)
        
        pseudo_heading = ('#number of pseudo atoms\n' + str(108) + 
			'\n#type          print    as     chem     oxidation' +
			'     mass       charge     polarization     ' +
			'B-factor     radii    connectivity     anisotropic' +
			'   anisotrop-type  tinker-type\n')
        pseudo_atoms.write(pseudo_heading)
        
	#100-ATOM LIBRARY	
        #make charges
        q = []
       	for k in range(100):
            q.append(0)
        for l in range(5*(100)):
            m = choice(range(100))
            n = choice(range(100))
            if m == n:
                n = choice(range(100))
            dq = random() * qmax
            if q[m] + dq <= qmax and q[n] - dq >= -1 * qmax:
                q[m] = float(float(q[m]) + dq)
                q[n] = float(float(q[n]) - dq)
            if q[m] > qmax or q[n] < -1 * qmax:
                q[m] = q[m] - dq
                q[n] = q[n] + dq
        for o in range(5*(100)):
            m = choice(range(100))
            n = choice(range(100))
            if m == n:
                n = choice(range(100))
            dq = random() * qmax
            if q[m] + dq <= qmax and q[n] - dq >= -1 * qmax:
                q[m] = float(float(q[m]) + dq)
                q[n] = float(float(q[n]) - dq)
            if q[m] > qmax or q[n] < -1 * qmax:
                q[m] = q[m] - dq
                q[n] = q[n] + dq
        p = choice(range(100))
        q[p] = q[p] - sum(q)
        if sum(q) != 0.000000000000000000000:
            for l in range(5*(100)):
                m = choice(range(100))
                n = choice(range(100))
                if m == n:
                    n = choice(range(100))
                dq = random() * qmax
                if q[m] + dq <= qmax and q[n] - dq >= -1 * qmax:
                    q[m] = float(float(q[m]) + dq)
                    q[n] = float(float(q[n]) - dq)
                if q[m] > qmax or q[n] < -1 * qmax:
                    q[m] = q[m] - dq
                    q[n] = q[n] + dq
            for o in range(5*(100)):
                m = choice(range(100))
                n = choice(range(100))
                if m == n:
                    n = choice(range(100))
                dq = random() * qmax
                if q[m] + dq <= qmax and q[n] - dq >= -1 * qmax:
                    q[m] = float(float(q[m]) + dq)
                    q[n] = float(float(q[n]) - dq)
                if q[m] > qmax or q[n] < -1 * qmax:
                    q[m] = q[m] - dq
                    q[n] = q[n] + dq
                p = choice(range(100))
                q[p] = q[p] - sum(q)
	
        #LJ parameters
	ep = []
	sig = []
        for i in range(100):
            epsilon = choice(epdim)
            ep.append(epsilon)
            sigma = choice(sigdim)
            sig.append(sigma)

        mat_charge = str(sum(q))
	cif_file.write('#NET CHARGE: ' + mat_charge + '\n')
	mat_X_stats = (mat_name + '     ' + str(nden_) + '     ' + str(xdim_) + '     ' + str(ydim_) +
			'     ' + str(zdim_) + '     ' + str(n_) + '     ' + 
			str(sum(q)) + '\n')
	mat_stats.write(mat_X_stats)
	

	#pseudo_atom loop
        for j in range(n_):
#FIX THIS TO ALLOW FOR NON-INT VALUES
            x = choice(range(int(xdim_ + 1)))
            y = choice(range(int(ydim_ + 1)))
            z = choice(range(int(zdim_ + 1)))
            atomtype = choice(range(100))
            #ep = choice(epdim)
            #sig = choice(sigdim)
            epval = ep[atomtype]
            sigval = sig[atomtype]
            charge = q[atomtype]
            if charge < 0:
                atom_X_cif = ('A' + str(atomtype) + '     ' + str(x) + '     ' +
				str(y) + '     ' + str(z) + '    ' +
				str(charge) + '\n')    
            	cif_file.write(atom_X_cif)
            if charge >= 0:
                atom_X_cif = ('A' + str(atomtype) + '     ' + str(x) + '     ' +
				str(y) + '     ' + str(z) + '     ' +
				str(charge) + '\n')
		cif_file.write(atom_X_cif)
             	
        for i in range(100):

            atom_X_mixing = ('A' + str(i) + '          LENNARD_JONES     ' +
				str(sig[i]) + '     ' + str(ep[i]) + '\n')
            mixing_rules.write(atom_X_mixing)

            atom_X_pseudo = ('A' + str(i) + '   yes   C   C   0   12.0   ' +
				str(q[i]) + '   0.0   0.0   1.0  1.00   0   ' +
				'0  absolute   0\n')
            pseudo_atoms.write(atom_X_pseudo)
 
#SUPPORTED ADSORBATES
# name         pseudo-atoms
# N2       :   N_n2; N_com
# CO2      :   C_co2; O_co2
# methane  :   CH4_sp3
# helium   :   He
# hydrogen :   H_h2; H_com
# H2       :   H_h2; H_com

        adsorbate_mixing = ('N_n2        LENNARD_JONES   36.0     3.31\n' +
			'N_com       none\n' +
			'C_co2       LENNARD_JONES   27.0     2.80\n' +
			'O_co2       LENNARD_JONES   79.0     3.05\n' +
			'CH4_sp3     LENNARD_JONES   158.5    3.72\n' +
			'He          LENNARD_JONES   10.9     2.64\n' +
			'H_h2        none\n' +
			'H_com       LENNARD_JONES   36.7     2.958\n' +
			'# general mixing rule for Lennard-Jones\n' +
			'Lorentz-Berthlot')
        mixing_rules.write(adsorbate_mixing)

        adsorbate_pseudo = ('N_n2     yes   N   N   0   14.00674   -0.4048' +
			'   0.0   1.0   0.7   0   0   relative   0\n' +
			'N_com    no    N   -   0   0.0         0.8096' +
			'   0.0   1.0   0.7   0   0   relative   0\n' +
			'C_co2    yes   C   C   0   12.0        0.70' +
			'     0.0   1.0   0.720 0   0   relative   0\n' +
			'O_co2    yes   O   O   0   15.9994    -0.35' +
			'     0.0   1.0   0.68  0   0   relative   0\n' +
			'CH4_sp3  yes   C   C   0   16.04246    0.0' +
			'      0.0   1.0   1.00  0   0   relative   0\n' +
			'He       yes   He  He  0   4.002602    0.0' +
			'      0.0   1.0   1.0   0   0   relative   0\n' +
			'H_h2     yes   H   H   0   1.00794     0.468' +
			'    0.0   1.0   0.7   0   0   relative   0\n' +
			'H_com    no    H   H   0   0.0        - 0.936' +
			'   0.0   1.0   0.7   0   0   relative   0\n')
        pseudo_atoms.write(adsorbate_pseudo)
        
        force_field_rules = ('# rules to overwrite\n0\n' +
				'# number of defined interactions\n0\n' +
				'# mixing rules to overwrite\n0')
	force_field.write(force_field_rules)
        
    cif_file.close()
    mixing_rules.close()
    pseudo_atoms.close()
    force_field.close()
    mat_stats.close()
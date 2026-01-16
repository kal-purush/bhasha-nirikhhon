from random import choice, random
import os
import subprocess

def mat12(N, ndenmax, ndenp=0.01, xmax=20, xp=0.01, ymax=20, yp=0.01, zmax=20,
zp=0.01, epmax=1, epp=0.01, sigmax=1, sigp=0.01, qmax=1):
    
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
    
    nden0 = drange(1, ndenmax, ndenp)
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

 	N_ = choice(ndendim)
	n_ = int(N_)
        xdim_ = choice(xdim)
        ydim_ = choice(ydim)
        zdim_ = choice(zdim)
        nden_ = xdim_ * ydim_ * zdim_ / N_
	
	#mat_stat_heading = ('\n\nBOUNDARIES\nNumber of particles: ' + ntag +
        #            	'\nx-coordinate: ' + xtag + '\ny-coordinate: ' + ytag 
#			+ '\nz-coordinate: ' + ztag + '\nEpsilon: ' + eptag + 
#			'\nSigma: ' + sigtag + '\nCharge: ' + qtag + '\n\n')
#	
#

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

	
        #ndendim_ = choice(ndendim)
        #xdim_ = choice(xdim)
        #ydim_ = choice(ydim)
        #zdim_ = choice(zdim)
        #nden_ = int(xdim_ * ydim_ * zdim_ / ndendim_)
	#densityinfo = 'mat '+str(i)+'  den  :   '+str(ndendim_)+'   x    :   '+str(xdim_)+'   y    :   '+str(ydim_)+'   z    :   '+str(zdim_)+'   vol  :   '+str(xdim_*ydim_*zdim_)+'   n    :   '+str(nden_)+'\n'
	#numden_file.write(densityinfo)	

        mixing_heading = ('# general rule for shifted vs truncated\nshifted\n' +
			'# general rule for tailcorrections\nno\n' +
			'# number of defined interactions\n' + str(n_+4) +  #check these + XXX values
			'\n# type interaction\n')
        mixing_rules.write(mixing_heading)
        
	#mixing_output.write(mixing_header)
        
        pseudo_heading = ('#number of pseudo atoms\n' + str(n_+9) + 
			'\n#type          print    as     chem     oxidation' +
			'     mass       charge     polarization     ' +
			'B-factor     radii    connectivity     anisotropic' +
			'   anisotrop-type  tinker-type\n')
        pseudo_atoms.write(pseudo_heading)
        	
        #sim_output.write('SimulationType                     MonteCarlo\nNumberofCycles                     ' + str(cycles) + '\nNumberofInitializationCycles       ' + str(initcycles) + '\nPrintEvery                         ' + str(printevery) + '\n\nForcefield                         GenericMOFs\nChargeMethod                       EWald\nCutOff                             15.0\n\nFramework  0\nFrameworkName  ' + str(mat_name) + '\nUnitCells  ' + str(unitcells[0]) + ' ' + str(unitcells[1]) + ' ' + str(unitcells[2]) + '\nExternalTemperature  ' + str(temperature) + '\nExternalPressure  ' + str(pressure) + '\n\nComponent 0  MoleculeName                      ' + str(gas) + '\n             MoleculeDefinition                TraPPE\n             TranslationProbability            1.0\n             ReinsertionProbability            1.0\n             SwapProbability                   1.0\n             CreateNumberOfMolecules           0')
        
        #make charges
        q = []
        for k in range(n_):
            q.append(0)
        for l in range(5*(n_)):
            m = choice(range(n_))
            n = choice(range(n_))
            if m == n:
                n = choice(range(n_))
            dq = random() * qmax
            if q[m] + dq <= qmax and q[n] - dq >= -1 * qmax:
                q[m] = float(float(q[m]) + dq)
                q[n] = float(float(q[n]) - dq)
            if q[m] > qmax or q[n] < -1 * qmax:
                q[m] = q[m] - dq
                q[n] = q[n] + dq
        for o in range(5*(n_)):
            m = choice(range(n_))
            n = choice(range(n_))
            if m == n:
                n = choice(range(n_))
            dq = random() * qmax
            if q[m] + dq <= qmax and q[n] - dq >= -1 * qmax:
                q[m] = float(float(q[m]) + dq)
                q[n] = float(float(q[n]) - dq)
            if q[m] > qmax or q[n] < -1 * qmax:
                q[m] = q[m] - dq
                q[n] = q[n] + dq
        p = choice(range(n_))
        q[p] = q[p] - sum(q)
        if sum(q) != 0.000000000000000000000:
            for l in range(5*(n_)):
                m = choice(range(n_))
                n = choice(range(n_))
                if m == n:
                    n = choice(range(n_))
                dq = random() * qmax
                if q[m] + dq <= qmax and q[n] - dq >= -1 * qmax:
                    q[m] = float(float(q[m]) + dq)
                    q[n] = float(float(q[n]) - dq)
                if q[m] > qmax or q[n] < -1 * qmax:
                    q[m] = q[m] - dq
                    q[n] = q[n] + dq
            for o in range(5*(n_)):
                m = choice(range(n_))
                n = choice(range(n_))
                if m == n:
                    n = choice(range(n_))
                dq = random() * qmax
                if q[m] + dq <= qmax and q[n] - dq >= -1 * qmax:
                    q[m] = float(float(q[m]) + dq)
                    q[n] = float(float(q[n]) - dq)
                if q[m] > qmax or q[n] < -1 * qmax:
                    q[m] = q[m] - dq
                    q[n] = q[n] + dq
                p = choice(range(n_))
                q[p] = q[p] - sum(q)

#redundancy probably isn't helping anything...
                #if sum(q) != 0.000000000000000000000:
                #    for l in range(5*(nden_ + 1)):
                #        m = choice(range(nden_ + 1))
                #        n = choice(range(nden_ + 1))
                #        if m == n:
                #            n = choice(range(nden_ + 1))
                #        dq = random() * qmax
                #        if q[m] + dq <= qmax and q[n] - dq >= -1 * qmax:
                #            q[m] = float(float(q[m]) + dq)
                #            q[n] = float(float(q[n]) - dq)
                #        if q[m] > qmax or q[n] < -1 * qmax:
                #            q[m] = q[m] - dq
                #            q[n] = q[n] + dq
                #    for o in range(5*(nden_ + 1)):
                #        m = choice(range(nden_ + 1))
                #        n = choice(range(nden_ + 1))
                #        if m == n:
                #            n = choice(range(nden_ + 1))
                #        dq = random() * qmax
                #        if q[m] + dq <= qmax and q[n] - dq >= -1 * qmax:
                #            q[m] = float(float(q[m]) + dq)
                #            q[n] = float(float(q[n]) - dq)
                #        if q[m] > qmax or q[n] < -1 * qmax:
                #            q[m] = q[m] - dq
                #            q[n] = q[n] + dq
                #    p = choice(range(nden_ + 1))
                #    q[p] = q[p] - sum(q)

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
            ep = choice(epdim)
            sig = choice(sigdim)
            
            charge = q[j]
            if charge < 0:
                atom_X_cif = ('A' + str(j) + '     ' + str(x) + '     ' +
				str(y) + '     ' + str(z) + '    ' +
				str(charge) + '\n')    
            	cif_file.write(atom_X_cif)
            if charge >= 0:
                atom_X_cif = ('A' + str(j) + '     ' + str(x) + '     ' +
				str(y) + '     ' + str(z) + '     ' +
				str(charge) + '\n')
		cif_file.write(atom_X_cif)
            
            atom_X_mixing = ('A' + str(j) + '          LENNARD_JONES     ' +
				str(sig) + '     ' + str(ep) + '\n')
            mixing_rules.write(atom_X_mixing)
	    
            atom_X_pseudo = ('A' + str(j) + '   yes   C   C   0   12.0   ' +
				str(q[j]) + '   0.0   0.0   1.0  1.00   0   ' +
				'0  absolute   0\n')
            pseudo_atoms.write(atom_X_pseudo)

        adsorbate_mixing = ('CH4_sp3     LENNARD_JONES          148.0  ' +
				'        3.73\nO_co2       LENNARD_JONES    ' +
				'      79.0          3.05\nC_co2       ' +
				'LENNARD_JONES          27.0          2.80\n' +
				'N_n2        LENNARD_JONES          36.0    ' +
				'      3.31\n' +
				'# general mixing rule for Lennard-Jones\n' +
				'Lorentz-Berthelot')
        mixing_rules.write(adsorbate_mixing)

        adsorbate_pseudo = ('CH4_sp3       yes      C      C        0   ' +
				'          16.04246   0.0        0.0        ' +
				'      1.0          1.00     0              ' +
				'  0             absolute        0\nCH3_sp3 ' +
				'      yes      C      C        0           ' +
				'  15.03452   0.0        0.0              ' +
				'1.0          1.00     0                0   ' +
				'          absolute        0\nCH2_sp3       ' +
				'yes      C      C        0             ' +
				'14.02658   0.0        0.0              1.0 ' +
				'         1.00     0                0       ' +
				'      absolute        0\nCH_sp3        yes ' +
				'     C      C        0             13.01864' +
				'   0.0        0.0              1.0         ' +
				' 1.00     0                0             ' +
				'absolute        0\nC_sp3         yes      ' +
				'C      C        0             12.0       ' +
				'0.0        0.0              1.0          ' +
				'1.00     0                0             ' +
				'absolute        0\nC_co2         yes      ' +
				'C      C        0             12.0107    ' +
				'0.7        1.508            1.0          ' +
				'0.720    0                0             ' +
				'absolute        0\nO_co2         yes      ' +
				'O      O        0             15.9994   ' +
				'-0.35       0.9475           1.0          ' +
				'0.68     0                0             ' +
				'relative        0\nN_n2          yes      ' +
				'N      N        0             14.00674  ' +
				'-0.482      0.0              1.0          ' +
				'0.7      0                0             ' +
				'relative        0\nN_com         no       ' +
				'N      N        0             0.0        ' +
				'0.964      0.0              1.0          ' +
				'0.7      0                0             ' +
				'relative        0\n')
        pseudo_atoms.write(adsorbate_pseudo)        
#run_output.write('#! /bin/sh -f\nexport RASPA_DIR=${HOME}/RASPA-2.0-CF/\n$RASPA_DIR/src/simulate $1')
        
        force_field_rules = ('# rules to overwrite\n0\n' +
				'# number of defined interactions\n0\n' +
				'# mixing rules to overwrite\n0')
	force_field.write(force_field_rules)

        #shell_output.write('source run\ncd Output/System_0\ngrep \'Average loading absolute\' $PATTERN * >> ~/RASPA-2.0-CF/mat6_output.txt\ngrep \'WARNING\' $PATTERN * >> ~/RASPA-2.0-CF/mat6_output.txt\ncd ..\ncd ..\nrm -rf Output\nrm shell\ncd ~/RASPA-2.0-CF')
        #sim_output.close()
        
    cif_file.close()
    mixing_rules.close()
    pseudo_atoms.close()
        #run_output.close()
    force_field.close()
        #shell_output.close()
    mat_stats.close()
# run file may vary depending on installation

#check what atoms are present in ch4 structure file
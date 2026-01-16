import json
import numpy as np
import sys
import os #may not need if xyz file shares the same directory as the program

#Adding feature to read from .xyz geometry file -AZ
#may or may not be read from the working directory

#Integrate within Nobel_Gas_model if desired @Ronit

#xyzfilename = sys.argv[2] #a cmd line argument

#if len(sys.argv) != 3:
#        print('Filename not specified. Please try again.')
#        exit()

#geom_file = os.path.join(xyzfilename) #adjust path if needed

#geom = np.genfromtxt(fname=geom_file,skip_header=2, dtype='unicode')
#atom_labels = geom[0:,0]
#atomic_coordinates = geom[0:,1:]

#atomic_coordinates = atomic_coordinates.astype(np.float)

#these were provided in the original porject code--should be generalized for any set of coordinates
atomic_coordinates = np.array([ [0.0,0.0,0.0], [3.0,4.0,5.0] ])
#atomic_coordinates = np.random.random([10,3])


class Nobel_Gas_model:
    def __init__(self, gas_model):
        self.gas_model = gas_model 
        self.model_parameters = self.assign_model_parameters()
        self.ionic_charge = 6
        self.orbital_types = ['s', 'px', 'py', 'pz']
        self.p_orbitals = self.orbital_types[1:]
        self.orbitals_per_atom = len(self.orbital_types)
        self.vec = {'px': [1.0,0.0,0.0], 'py' : [0.0, 1.0, 0.0], 'pz' : [0.0, 0.0, 1.0]}
        self.orbital_occupations = {'s' : 0, 'px' : 1, 'py' : 1, 'pz' : 1 }

    def assign_model_parameters(self):
        filename = self.gas_model+'.txt'
        commands = {}
        with open(filename) as fh:
            for line in fh:
                command, description = line.strip().split(',', 1)
                commands[command] = float(description.strip())
        return commands

    def orb(self, ao_index):
        orb_index = ao_index % self.orbitals_per_atom
        return self.orbital_types[orb_index]
    def atom(self, ao_index):
        return ao_index // self.orbitals_per_atom
    def ao_index(self,atom_p, orb_p):
        p = atom_p * self.orbitals_per_atom
        p += self.orbital_types.index(orb_p)
        return p

# system = Nobel_Gas_model()
# print(system)
# print(system.model_parameters)

# for index in range(2*system.orbitals_per_atom):
#     print('index',index,'atom',system.atom(index),'orbital',system.orb(index))
#
# print('index test:')
# for index in range(2*system.orbitals_per_atom):
#     atom_p = system.atom(index)
#     orb_p = system.orb(index)
#     print(index, system.ao_index(atom_p,orb_p))

 #see above
 #atomic_coordinates = np.array([ [0.0,0.0,0.0], [3.0,4.0,5.0] ])

def hopping_energy(o1, o2, r12, model_parameters, system):
    r12_rescaled = r12 / model_parameters['r_hop']
    r12_length = np.linalg.norm(r12_rescaled)
    ans = np.exp( 1.0 - r12_length**2 )
    if o1 == 's' and o2 == 's':
        ans *= model_parameters['t_ss']
    if o1 == 's' and o2 in system.p_orbitals:
        ans *= np.dot(system.vec[o2], r12_rescaled) * model_parameters['t_sp']
    if o2 == 's' and o1 in system.p_orbitals:
        ans *= -np.dot(system.vec[o1], r12_rescaled)* model_parameters['t_sp']
    if o1 in system.p_orbitals and o2 in system.p_orbitals:
        ans *= ( (r12_length**2) * np.dot(system.vec[o1], system.vec[o2]) * model_parameters['t_pp2']
                 - np.dot(system.vec[o1], r12_rescaled) * np.dot(system.vec[o2], r12_rescaled)
                 * ( model_parameters['t_pp1'] + model_parameters['t_pp2'] ) )
    return ans

def coulomb_energy(o1, o2, r12, system):
    '''Returns the Coulomb matrix element for a pair of multipoles of type o1 & o2 separated by a system.vector r12.'''
    r12_length = np.linalg.norm(r12)
    if o1 == 's' and o2 == 's':
        ans = 1.0 / r12_length
    if o1 == 's' and o2 in system.p_orbitals:
        ans = np.dot(system.vec[o2], r12) / r12_length**3
    if o2 == 's' and o1 in system.p_orbitals:
        ans = -np.dot(system.vec[o1], r12) / r12_length**3
    if o1 in system.p_orbitals and o2 in system.p_orbitals:
        ans = ( np.dot(system.vec[o1], system.vec[o2]) / r12_length**3
               - 3.0 * np.dot(system.vec[o1], r12) * np.dot(system.vec[o2], r12) / r12_length**5 )
    return ans

def pseudopotential_energy(o, r, model_parameters, system):
    '''Returns the energy of a pseudopotential between a multipole of type o and an atom separated by a system.vector r.'''
    ans = model_parameters['v_pseudo']
    r_rescaled = r / model_parameters['r_pseudo']
    r_length = np.linalg.norm(r_rescaled)
    ans *= np.exp( 1.0 - r_length**2 )
    if o in system.p_orbitals:
        ans *= -2.0 * np.dot(system.vec[o], r_rescaled)
    return ans

def calculate_energy_ion(atomic_coordinates, system):
    '''Returns the ionic contribution to the total energy for an input list of atomic coordinates.'''
    energy_ion = 0.0
    for i, r_i in enumerate(atomic_coordinates):
        for j, r_j in enumerate(atomic_coordinates):
            if i < j:
                energy_ion += (system.ionic_charge**2)*coulomb_energy('s', 's', r_i - r_j, system)
    return energy_ion

def calculate_potential_vector(atomic_coordinates, model_parameters, system):
    '''Returns the electron-ion potential energy system.vector for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*system.orbitals_per_atom
    potential_vector = np.zeros(ndof)
    for p in range(ndof):
        potential_vector[p] = 0.0
        for atom_i,r_i in enumerate(atomic_coordinates):
            r_pi = atomic_coordinates[system.atom(p)] - r_i
            if atom_i != system.atom(p):
                potential_vector[p] += ( pseudopotential_energy(system.orb(p), r_pi, system.model_parameters, system)
                                         - system.ionic_charge * coulomb_energy(system.orb(p), 's', r_pi, system) )
    return potential_vector

def calculate_interaction_matrix(atomic_coordinates, model_parameters, system):
    '''Returns the electron-electron interaction energy matrix for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*system.orbitals_per_atom
    interaction_matrix = np.zeros( (ndof,ndof) )
    for p in range(ndof):
        for q in range(ndof):
            if system.atom(p) != system.atom(q):
                r_pq = atomic_coordinates[system.atom(p)] - atomic_coordinates[system.atom(q)]
                interaction_matrix[p,q] = coulomb_energy(system.orb(p), system.orb(q), r_pq, system)
            if p == q and system.orb(p) == 's':
                interaction_matrix[p,q] = system.model_parameters['coulomb_s']
            if p == q and system.orb(p) in system.p_orbitals:
                interaction_matrix[p,q] = system.model_parameters['coulomb_p']
    return interaction_matrix



def chi_on_atom(o1, o2, o3, model_parameters):
    '''Returns the value of the chi tensor for 3 orbital indices on the same atom.'''
    if o1 == o2 and o3 == 's':
        return 1.0
    if o1 == o3 and o3 in system.p_orbitals and o2 == 's':
        return model_parameters['dipole']
    if o2 == o3 and o3 in system.p_orbitals and o1 == 's':
        return model_parameters['dipole']
    return 0.0

def calculate_chi_tensor(atomic_coordinates, model_parameters, system):
    '''Returns the chi tensor for an input list of atomic coordinates'''
    ndof = len(atomic_coordinates)*system.orbitals_per_atom
    chi_tensor = np.zeros( (ndof,ndof,ndof) )
    for p in range(ndof):
        for orb_q in system.orbital_types:
            q = system.ao_index(system.atom(p), orb_q) # p & q on same atom
            for orb_r in system.orbital_types:
                r = system.ao_index(system.atom(p), orb_r) # p & r on same atom
                chi_tensor[p,q,r] = chi_on_atom(system.orb(p), system.orb(q), system.orb(r), system.model_parameters)
    return chi_tensor

 #print('chi =\n',chi_tensor)

def calculate_hamiltonian_matrix(atomic_coordinates, model_parameters, system):
    '''Returns the 1-body Hamiltonian matrix for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*system.orbitals_per_atom
    hamiltonian_matrix = np.zeros( (ndof,ndof) )
    potential_vector = calculate_potential_vector(atomic_coordinates, system.model_parameters, system)
    for p in range(ndof):
        for q in range(ndof):
            if system.atom(p) != system.atom(q):
                r_pq = atomic_coordinates[system.atom(p)] - atomic_coordinates[system.atom(q)]
                hamiltonian_matrix[p,q] = hopping_energy(system.orb(p), system.orb(q), r_pq, system.model_parameters, system)
            if system.atom(p) == system.atom(q):
                if p == q and system.orb(p) == 's':
                    hamiltonian_matrix[p,q] += system.model_parameters['energy_s']
                if p == q and system.orb(p) in system.p_orbitals:
                    hamiltonian_matrix[p,q] += system.model_parameters['energy_p']
                for orb_r in system.orbital_types:
                    r = system.ao_index(system.atom(p), orb_r)
                    hamiltonian_matrix[p,q] += ( chi_on_atom(system.orb(p), system.orb(q), orb_r, system.model_parameters)
                                                 * potential_vector[r] )
    return hamiltonian_matrix

def calculate_atomic_density_matrix(atomic_coordinates, system):
    '''Returns a trial 1-electron density matrix for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*system.orbitals_per_atom
    density_matrix = np.zeros( (ndof,ndof) )
    for p in range(ndof):
        density_matrix[p,p] = system.orbital_occupations[system.orb(p)]
    return density_matrix


class Hartree_Fock:
    def __init__(self, hamiltonian_matrix, interaction_matrix, density_matrix, chi_tensor):
        self.hamiltonian_matrix = hamiltonian_matrix
        self.interaction_matrix = interaction_matrix
        self.chi_tensor = chi_tensor
        self.fock_matrix = self.calculate_fock_matrix(density_matrix)
        self.density_matrix = self.calculate_density_matrix(self.fock_matrix, system)
        self.conv_density_matrix, self.conv_fock_matrix = self.scf_cycle(*scf_params) 
        self.energy_scf = self.calculate_energy_scf()

    def calculate_fock_matrix(self, density_matrix):
        '''Returns the Fock matrix defined by the input Hamiltonian, interaction, & density matrices.

        Parameters
        ----------
        hamiltonian_matrix : numpy.array
            A 2D array of 1-body Hamiltonian matrix elements.
        interaction_matrix : numpy.array
            A 2D array of electron-electron interaction matrix elements.
        density_matrix : numpy.array
            A 2D array of 1-electron densities.
        chi_tensor : numpy.array
            A 3D array for the chi tensor, a 3-index tensor of p, q, and r. p and q are the atomic orbital indices and r is the multipole moment index.

        Returns
        -------
        fock_matrix : numpy.array
            A 2D array of Fock matrix elements.
        '''
        fock_matrix = self.hamiltonian_matrix.copy()
        fock_matrix += 2.0 * np.einsum('pqt,rsu,tu,rs',
                                       self.chi_tensor,
                                       self.chi_tensor,
                                       self.interaction_matrix,
                                       density_matrix,
                                       optimize=True)
        fock_matrix -= np.einsum('rqt,psu,tu,rs',
                                 self.chi_tensor,
                                 self.chi_tensor,
                                 self.interaction_matrix,
                                 density_matrix,
                                 optimize=True)
        return fock_matrix

    def calculate_density_matrix(self, fock_matrix, system):
        '''Returns the 1-electron density matrix defined by the input Fock matrix.

           Parameters
           ----------
           fock_matrix : np.array 
               The fock matrix is a numpy array of size (ndof,ndof)

           Returns
           -------
           density_matrix : np.array
               The density matrix is a numpy array of size (ndof,ndof) that is the product of the
               occupied MOs with the transpose of the occupied MOs.
           
        '''
        num_occ = (system.ionic_charge // 2) * np.size(fock_matrix,
                                                0) // system.orbitals_per_atom
        orbital_energy, orbital_matrix = np.linalg.eigh(fock_matrix)
        occupied_matrix = orbital_matrix[:, :num_occ]
        density_matrix = occupied_matrix @ occupied_matrix.T
        return density_matrix

    def scf_cycle(self, max_scf_iterations = 100, mixing_fraction = 0.25, convergence_tolerance = 1e-4):
        '''Returns converged density & Fock matrices defined by the input Hamiltonian, interaction, & density matrices.

           Parameters
           ----------
           hamiltonian_matrix : np.array
               This is the hamiltonain matrix as a numpy array of size(ndof,ndof)
           interaction_matrix : np.array
               This is the interaction matrix as a numpy array of size(ndof,ndof)
           density_matrix : np.array
               this is the MO density matrix as a numpy array of size(ndof,ndof)
           chi_tensor : np.array
               This is th chi tensor as a numpy array of size(ndof,ndof,ndof)
           max_scf_iteration : int,optional
               This is the maximum number of iterations that the Cycle should take to try and converge. Default is 100 

           Returns
           -------
           new_density_matrix: np.array
               This is returned either as the converged density or non-converged if max_iterations is passed,
               it is a numpy array of size(ndof,ndof) 
           new_fock_matrix: np.array
               This is either the converged fock matrix or non-converged if max_iterations is passed and the
               warning is printed. The output array is of size(ndof,ndof)
           '''

        old_density_matrix = self.density_matrix.copy()
        for iteration in range(max_scf_iterations):
            new_fock_matrix = self.calculate_fock_matrix(old_density_matrix)
            new_density_matrix = self.calculate_density_matrix(new_fock_matrix, system)

            error_norm = np.linalg.norm( old_density_matrix - new_density_matrix )
            if error_norm < convergence_tolerance:
                return new_density_matrix, new_fock_matrix

            old_density_matrix = (mixing_fraction * new_density_matrix
                                  + (1.0 - mixing_fraction) * old_density_matrix)
        print("WARNING: SCF cycle didn't converge")
        return new_density_matrix, new_fock_matrix

    def calculate_energy_scf(self):
        '''Returns the Hartree-Fock total energy defined by the input Hamiltonian, Fock, & density matrices.

           Parameters
           ----------
           hamiltonian_matrix : np.array
               This is the hamiltoian matrix calculated in calculate_hamiltonian_matrix, it is a numpy array of size(ndof,ndof)
           fock_matrix : np.array
               This is the fock matrix calculated in scf_cycle, it is a nupmy array of size (ndof,ndof)
           density_marix : np.array           
               This is the density matrix calculated in scf_cycle, it is a nupmy array of size (ndof,ndof)

           Returns
           -------
           energy_scf : float
               This is the energy of the ground state of the atoms from the SCF calcuation. It is ouput as a float.
        '''
        #self.density_matrix, self.fock_matrix = self.scf_cycle(*scf_params)
        print("Density matrix and Fock matrix calculated.")
        energy_scf = np.einsum('pq,pq', self.hamiltonian_matrix + self.conv_fock_matrix, self.conv_density_matrix)
        return energy_scf


class MP2():
    def __init__(self, energy_ion, atomic_coordinates):
        #super().__init__(chi_tensor, interaction_matrix, energy_scf)
        
        self.conv_fock_matrix = calc.conv_fock_matrix
        self.energy_ion = energy_ion
        self.atomic_coordinates = atomic_coordinates
        self.occupied_energy, self.virtual_energy, self.occupied_matrix, self.virtual_matrix = self.partition_orbitals()
        self.interaction_tensor = self.transform_interaction_tensor()
        self.mp2_energy = self.calculate_energy_mp2()
        self.total_energy = self.calculate_total_energy()

    def partition_orbitals(self):
        """Returns a list with the occupied/virtual energies & orbitals defined by the input Fock matrix.
	Parameters
    	----------
    	fock_matrix : numpy.ndarray
        	A (ndof,ndof) array populated with 'float' data types. The elements of this matrix are constructed by
        	numerically computing an expectation value for the fock operator operating on a state vector.
        	The transformed state is then projected onto an orbital basis.
    	Returns
    	-------
    	occupied_energy : numpy.ndarray
        	A [:numocc] long array containing the eigenvalues that correspond to the
        	eigenvectors of the occupied orbital space. The stored values are 'floats'.
    	virtual_energy : numpy.ndarray
        	A (numocc:) long array containing the eigenvalues that correspond to the
        	eigenvectors of the virtual orbital space. The stored values are 'floats'.
    	occupied_matrix : numpy.ndarray
        	A rank 2 array, [:, :num_occ], indexed by the number of basis functions/molecular
        	orbitals and the number of occupied orbitals. The stored values are 'floats'.
    	virtual_matrix : numpy.ndarray
        	A rank 2 array, [:, num_occ:], indexed by the number of virtual orbitals and by the
        	number of basis functions/molecular orbitals. The stored values are 'floats'.
        """
        num_occ = ((system.ionic_charge // 2) * np.size(self.conv_fock_matrix, 0) // system.orbitals_per_atom)
        orbital_energy, orbital_matrix = np.linalg.eigh(self.conv_fock_matrix)
        occupied_energy = orbital_energy[:num_occ]
        virtual_energy = orbital_energy[num_occ:]
        occupied_matrix = orbital_matrix[:, :num_occ]
        virtual_matrix = orbital_matrix[:, num_occ:]
        
        return occupied_energy, virtual_energy, occupied_matrix, virtual_matrix

    def transform_interaction_tensor(self):
        
        chi2_tensor = np.einsum('qa,ri,qrp', self.virtual_matrix, self.occupied_matrix, calc.chi_tensor, optimize=True)
        interaction_tensor = np.einsum('aip,pq,bjq->aibj', chi2_tensor, calc.interaction_matrix, chi2_tensor, optimize=True)
        return interaction_tensor
    
    def calculate_energy_mp2(self):
        num_occ =  ((system.ionic_charge // 2) * np.size(self.conv_fock_matrix, 0) // system.orbitals_per_atom)
        num_virt = (len(self.atomic_coordinates) * system.orbitals_per_atom) - num_occ
        energy_mp2 = 0.0
        num_occ = len(self.occupied_energy)
        num_virt = len(self.virtual_energy)
        for a in range(num_virt):
            for b in range(num_virt):
                for i in range(num_occ):
                    for j in range(num_occ):
                        energy_mp2 -= ((2.0 * self.interaction_tensor[a, i, b, j]**2 - self.interaction_tensor[a, i, b, j] * 
					self.interaction_tensor[a, j, b, i]) / (self.virtual_energy[a] + self.virtual_energy[b] - 
					self.occupied_energy[i] - self.occupied_energy[j]))
        return energy_mp2

    def calculate_total_energy(self):

        total_energy = calc.energy_scf + self.energy_ion + self.mp2_energy
        return total_energy


                                                                   
if __name__ == "__main__":
    scf_params = []
    try:
        scf_params.append(int(input("Maximum number of scf iterations (default = 100):\n")))
    except:
        pass
    try:
        scf_params.append(float(input("Mixing fraction (default = 0.25):\n")))
    except:
        pass
    try:
        scf_params.append(float(input("Convergence_tolerance (default = 1e-4):\n")))
    except:
        pass

    #Nobel_Gas_model.gas_name = 'argon'
    system = Nobel_Gas_model(sys.argv[1])
    interaction_matrix = calculate_interaction_matrix(atomic_coordinates, system.model_parameters, system)
    chi_tensor = calculate_chi_tensor(atomic_coordinates, system.model_parameters, system)
    hamiltonian_matrix = calculate_hamiltonian_matrix(atomic_coordinates, system.model_parameters, system)
    density_matrix = calculate_atomic_density_matrix(atomic_coordinates, system)
    energy_ion = calculate_energy_ion(atomic_coordinates, system)
    calc = Hartree_Fock(hamiltonian_matrix, interaction_matrix, density_matrix, chi_tensor)
    print('SCF energy: ',calc.energy_scf)
    print('Total Energy before MP2 correction: ', calc.energy_scf + energy_ion)
    mp2 = MP2(energy_ion, atomic_coordinates)
    print('MP2 second order correction: ',mp2.mp2_energy)
    print('Total Energy after MP2 correction: ', mp2.total_energy)
#    universe.py(str(sys.argv[2]))
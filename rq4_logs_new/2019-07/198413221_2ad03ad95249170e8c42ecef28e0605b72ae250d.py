import json
import numpy as np
import sys

#import pandas as pd

#filename = 'data.txt'

#commands = {}
#with open(filename) as fh:
#    for line in fh:
#        command, description = line.strip().split(' ', 1)
#        commands[command] = float(description.strip())

#model_parameters = json.dumps(commands, indent=2, sort_keys=True)
#model_parameters = commands


class Nobel_Gas_model:
    def __init__(self):
       # if isinstance(gas, str): 
       #     self.gas = gas
       # else:
        
       #     raise TypeError('Gas Name should be str')

        self.model_parameters = self.assign_model_parameters()
        self.ionic_charge = 6 
        self.orbital_types = ['s', 'px', 'py', 'pz']
        self.p_orbitals = self.orbital_types[1:]
        self.orbitals_per_atom = len(self.orbital_types)
        self.vec = {'px': [1.0,0.0,0.0], 'py' : [0.0, 1.0, 0.0], 'pz' : [0.0, 0.0, 1.0]}
        self.orbital_occupations = {'s' : 0, 'px' : 1, 'py' : 1, 'pz' : 1 }
    
    def assign_model_parameters(self):
        filename = sys.argv[1]
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


#    def __str__(self):
#        return "The Model Parameters are for the gas " + self.gas 

#---------------------------------------------------#
system = Nobel_Gas_model()
#print(system)
print(system.model_parameters)

for index in range(2*system.orbitals_per_atom):
    print('index',index,'atom',system.atom(index),'orbital',system.orb(index))

print('index test:')
for index in range(2*system.orbitals_per_atom):
    atom_p = system.atom(index)
    orb_p = system.orb(index)
    print(index, system.ao_index(atom_p,orb_p))
#----------------------------------------------------#


###############################################
# 1. Define Atomic Coordinates 
atomic_coordinates = np.array([ [0.0,0.0,0.0], [3.0,4.0,5.0] ])


# 2. Define ndof, since its used in a lot of functions
################################################

def hopping_energy(o1, o2, r12, model_parameters):
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

def coulomb_energy(o1, o2, r12):
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



def pseudopotential_energy(o, r, model_parameters):
    '''Returns the energy of a pseudopotential between a multipole of type o and an atom separated by a system.vector r.'''
    ans = model_parameters['v_pseudo']
    r_rescaled = r / model_parameters['r_pseudo']
    r_length = np.linalg.norm(r_rescaled)
    ans *= np.exp( 1.0 - r_length**2 )
    if o in system.p_orbitals:
        ans *= -2.0 * np.dot(system.vec[o], r_rescaled)
    return ans





def calculate_energy_ion(atomic_coordinates):
    '''Returns the ionic contribution to the total energy for an input list of atomic coordinates.'''
    energy_ion = 0.0
    for i, r_i in enumerate(atomic_coordinates):
        for j, r_j in enumerate(atomic_coordinates):
            if i < j:
                energy_ion += (ionic_charge**2)*coulomb_energy('s', 's', r_i - r_j)
    return energy_ion








def calculate_potential_vector(atomic_coordinates, model_parameters):
    '''Returns the electron-ion potential energy system.vector for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*system.orbitals_per_atom
    potential_vector = np.zeros(ndof)
    for p in range(ndof):
        potential_vector[p] = 0.0
        for atom_i,r_i in enumerate(atomic_coordinates):
            r_pi = atomic_coordinates[system.atom(p)] - r_i
            if atom_i != system.atom(p):
                potential_vector[p] += ( pseudopotential_energy(system.orb(p), r_pi, system.model_parameters)
                                         - system.ionic_charge * coulomb_energy(system.orb(p), 's', r_pi) )
    return potential_vector









def calculate_interaction_matrix(atomic_coordinates, model_parameters):
    '''Returns the electron-electron interaction energy matrix for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*system.orbitals_per_atom
    interaction_matrix = np.zeros( (ndof,ndof) )
    for p in range(ndof):
        for q in range(ndof):
            if system.atom(p) != system.atom(q):
                r_pq = atomic_coordinates[system.atom(p)] - atomic_coordinates[system.atom(q)]
                interaction_matrix[p,q] = coulomb_energy(system.orb(p), system.orb(q), r_pq)
            if p == q and system.orb(p) == 's':
                interaction_matrix[p,q] = system.model_parameters['coulomb_s']
            if p == q and system.orb(p) in system.p_orbitals:
                interaction_matrix[p,q] = system.model_parameters['coulomb_p']                
    return interaction_matrix

interaction_matrix = calculate_interaction_matrix(atomic_coordinates, system.model_parameters)
#print('V_ee =\n', interaction_matrix)









def chi_on_atom(o1, o2, o3, model_parameters):
    '''Returns the value of the chi tensor for 3 orbital indices on the same atom.'''
    if o1 == o2 and o3 == 's':
        return 1.0
    if o1 == o3 and o3 in system.p_orbitals and o2 == 's':
        return model_parameters['dipole']
    if o2 == o3 and o3 in system.p_orbitals and o1 == 's':
        return model_parameters['dipole']
    return 0.0

def calculate_chi_tensor(atomic_coordinates, model_parameters):
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

chi_tensor = calculate_chi_tensor(atomic_coordinates, system.model_parameters)
#print('chi =\n',chi_tensor)
    


def calculate_hamiltonian_matrix(atomic_coordinates, model_parameters):
    '''Returns the 1-body Hamiltonian matrix for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*system.orbitals_per_atom
    hamiltonian_matrix = np.zeros( (ndof,ndof) )
    potential_vector = calculate_potential_vector(atomic_coordinates, system.model_parameters)
    for p in range(ndof):
        for q in range(ndof):
            if system.atom(p) != system.atom(q):
                r_pq = atomic_coordinates[system.atom(p)] - atomic_coordinates[system.atom(q)]
                hamiltonian_matrix[p,q] = hopping_energy(system.orb(p), system.orb(q), r_pq, system.model_parameters)
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
hamiltonian_matrix = calculate_hamiltonian_matrix(atomic_coordinates, system.model_parameters)
#print('Hamiltonian Matrix = \n', hamiltonian_matrix)



def calculate_atomic_density_matrix(atomic_coordinates):
    '''Returns a trial 1-electron density matrix for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*system.orbitals_per_atom
    density_matrix = np.zeros( (ndof,ndof) )
    for p in range(ndof):
        density_matrix[p,p] = system.orbital_occupations[system.orb(p)]
    return density_matrix

density_matrix = calculate_atomic_density_matrix(atomic_coordinates)
#print('atomic density matrix =\n', density_matrix)


## Hartree Fock Class#######################################


                   
                                                                                           
                                                    
                                                    
                                    
                                                                     
                                                                             
                                                     
                                                    

                                                    
                                                                                                     
                  
                  
                                        
                                                             
                                        
                                                                        
                                    
                                               
                                
                                                                                                                                                       
               
               
                                 
                                               
           
                                                    
                                                       
                                                       
                                                       
                                                               
                                                      
                                                     
                                                 
                                                 
                                                 
                                                         
                                                
                                               
                          

                                                    
                                                                                  
                     
                     
                                 
                                                                   
                  
                  
                                    
                                                                                                 
                                                                   

           
                                                                                                   
                                                                    
                                                     
                                                            
                             

                                                                                                        
                                                                                                                       
                     
                     
                                        
                                                                                 
                                        
                                                                                 
                                    
                                                                                
                                
                                                                             
                                           
                                                                                                                      
                  
                  
                                       
                                                                                                             
                                                     
                                    
                                                                                                            
                                                                         
              

                                                       
                                                   
                                                                            
                                                                               

                                                                                  
                                                  
                                                          

                                                                      
                                                                                 
                                                   
                                                  

                                   
                                                                                                            
                     
                     
                                        
                                                                                                                               
                                 
                                                                                                       
                                   
                                                                                                          
                  
                  
                             
                                                                                                                   
           
                                                                           
                                                           
                                                                                                        
                         

                           
                                                




                          
                                                                                              


#!/usr/bin/env python
# coding: utf-8

# # 2019 MolSSI Summer School QM project: semiempirical model of Argon
# 
# ## 1. Introduction
# 
# In this project, we will simulate a cluster of Argon atoms using quantum mechanics (QM). First-principles (a.k.a. ab initio) QM simulations are complicated and expensive, and a quick implementation would rely on a substantial amount of pre-existing software infrastructure (e.g. PySCF or Psi4). Instead, we will implement a much simpler semiempirical QM simulation that has been designed and parameterized to reproduce first-principles QM data using a minimal model. We can then limit our external dependencies to the standard numerical functionality of Python:

# In[1]:


import numpy as np


# As is typically the case in quantum chemistry, we will input a set of atomic coordinates $\vec{r}_i$ and compute the ground state energy of Argon atoms with those coordinates. All physical quantities in this project will be in Hartree atomic units, where the bohr is the unit of length and the hartree is the unit of energy.

# In[2]:


atomic_coordinates = np.array([ [0.0,0.0,0.0], [3.0,4.0,5.0] ])
number_of_atoms = len(atomic_coordinates)

print('coordinates =\n',atomic_coordinates)
print('# of atoms =',number_of_atoms)


# More complicated and featureful software would be able to compute other properties besides the ground state energy like atomic forces. It would also have more convenient options for specifying inputs and returning outputs.
# 
# Note that this QM project is primarily meant as a programming exercise. It contains a theoretical specification of a model, and we will implement the components of the model in software as they are specified. If you have a strong background in quantum chemistry, then you should find the theory reasonably familiar. However, it is only really necessary for you to understand each mathematical expression one-at-a-time to effectively implement this software.
# 
# ## 2. Model Hamiltonian
# 
# As is standard in quantum chemistry, we will assume that the total energy of our system is defined to be the ground state energy of a quantum many-body Hamiltonian $\hat{H}$. In second quantization notation, we can write it as
# 
# $$ \hat{H} = E_{\mathrm{ion}} + \sum_{p,q} \sum_{\sigma \in \{ \uparrow , \downarrow \} } h_{p,q} \hat{a}_{p,\sigma}^{\dagger} \hat{a}_{q,\sigma} + \tfrac{1}{2}\sum_{p,q,r,s} \sum_{\sigma,\sigma' \in \{ \uparrow , \downarrow \} } V_{p,q,r,s} \hat{a}_{p,\sigma}^{\dagger} \hat{a}_{r,\sigma'}^{\dagger} \hat{a}_{s,\sigma'} \hat{a}_{q,\sigma} , $$
# 
# where $\hat{a}_{p,\sigma}^{\dagger}$ and $\hat{a}_{p,\sigma}$ are the electron raising and lowering operators for an atomic orbital index $p$ and spin $\sigma$. We will not be using $\hat{H}$ itself in our calculations, but we will make use of the coefficient tensors $h_{p,q}$ and $V_{p,q,r,s}$. In first-principles calculations, each element of $h_{p,q}$ and $V_{p,q,r,s}$ would require the evaluation of a complicated integral. In our semiempirical model, we will set most of them to zero and assign a simple analytical form to the rest of them. The notation being used here is mostly consistent with modern quantum chemistry notation, but some objects, particularly $V_{p,q,r,s}$, have multiple conventions in practice.
# 
# ### A. Model design & parameters
# 
# This semiempirical model combines some standard concepts and methods used in physics and chemistry. First, it will use a minimal number of electronic degrees of freedom. Because Argon is a noble gas, it interacts primarily through London dispersion forces that are mediated by quantum dipole fluctuations. The lowest energy dipole transition is from the occupied $3p$ states to the unoccupied $4s$ state, and we will include these 4 atomic orbitals per atom. Similarly, we will use a multipole expansion to simplify electronic excitations and retain only the monopole and dipole terms, which also restricts electronic polarization to 4 degrees of freedom per atom. We will use $\{s, p_x, p_y, p_z\}$ to label both atomic orbitals and multipole moments on each atom. The nuclear charge of Argon is 18, but our model combines the nucleus and the 12 neglected electrons ($1s^2$, $2s^2$, $2p^6$, and $3s^2$) into an ionic point charge with $Z = 6$.

# In[3]:


ionic_charge = 6
orbital_types = ['s', 'px' ,'py', 'pz']
orbitals_per_atom = len(orbital_types)

p_orbitals = orbital_types[1:]
print('all orbitals =', orbital_types)
print('p orbitals =', p_orbitals)


# The index of an atomic orbital specifies which atom it is located on and what type it is. We will often extract these individual pieces of information using $\mathrm{atom}(p)$ to denote the atom's index and $\mathrm{orb}(p)$ to denote the orbital type. This is the first of many instances in this project where we could either represent something as a pre-tabulate list or a function. We will always make the simpler choice, in this case functions:

# In[52]:


def atom(ao_index):
    '''Returns the atom index part of an atomic orbital index.'''
    return ao_index // orbitals_per_atom

def orb(ao_index):
    '''Returns the orbital type of an atomic orbital index.'''
    orb_index = ao_index % orbitals_per_atom
    return orbital_types[orb_index]

def ao_index(atom_p,orb_p):
    '''Returns the atomic orbital index for a given atom index and orbital type.'''
    p = atom_p*orbitals_per_atom
    p += orbital_types.index(orb_p)
    return p

for index in range(number_of_atoms*orbitals_per_atom):
    print('index',index,'atom',atom(index),'orbital',orb(index))

print('index test:')
for index in range(number_of_atoms*orbitals_per_atom):
    print(index, ao_index(atom(index),orb(index)))


# We will discuss the model parameters in more detail as they are used, but it is a good idea to first collect them all in a common data structure, a Python dictionary, for convenient access throughout the notebook:

# In[5]:


# REMINDER: atomic units w/ energies in hartree, distances in bohr
model_parameters = { 'r_hop' : 3.1810226927827516, # hopping length scale
                     't_ss' : 0.03365982238611262, # s-s hopping energy scale
                     't_sp' : -0.029154833035109226, # s-p hopping energy scale
                     't_pp1' : -0.0804163845390335, # 1st p-p hopping energy scale
                     't_pp2' : -0.01393611496959445, # 2nd p-p hopping energy scale
                     'r_pseudo' : 2.60342991362958, # pseudopotential length scale
                     'v_pseudo' : 0.022972992186364977, # pseudopotential energy scale
                     'dipole' : 2.781629275106456, # dipole strength of s-p transition
                     'energy_s' : 3.1659446174413004, # onsite energy of s orbital
                     'energy_p' : -2.3926873325346554, # onsite energy of p orbital
                     'coulomb_s' : 0.3603533286088998, # Coulomb self-energy of monopole
                     'coulomb_p' : -0.003267991835806299 } # Coulomb self-energy of dipole


# There are no parameters related to orbital overlap because all atomic orbitals are assumed to be orthogonal. The parameter values have been pre-optimized for this project, but the fitting process and reference data are both listed at the end of the project if you'd like to learn more about them.
# 
# ### B. Slater-Koster tight-binding model
# 
# We will describe the kinetic energy of electrons using a simplified [Slater-Koster tight-binding method](https://en.wikipedia.org/wiki/Tight_binding). Because of the symmetry of atomic orbitals and the translational invariance of the kinetic energy operator, there are 4 distinct, distance-dependent "hopping" energies that characterize the interatomic kinetic energy between s and p orbitals:
# 
# ![s-p hopping diagram](hopping_cases.png)
# 
# All other atomic orientations can be related to these cases by a change of coordinates. While it is compatible with very general functional forms, we will use a Gaussian form to simplify the model and its implementation. The distance-dependence of this simple version is controlled by a single hopping length scale $r_{\mathrm{hop}}$ and the strength of each type of hopping energy,
# 
# $$ t_{o,o'}(\vec{r}) = \exp(1-r^2/r_{\mathrm{hop}}^2) \times \begin{cases}
#  t_{ss} , & o = o' = s \\
#  [\vec{o}' \cdot (\vec{r}/r_{\mathrm{hop}})] t_{sp}, & o = s \ \& \ o' \in \{p_x, p_y, p_z\} \\
#  -[\vec{o} \cdot (\vec{r}/r_{\mathrm{hop}})] t_{sp} , & o' = s \ \& \ o \in \{p_x, p_y, p_z\} \\
#  (r^2/r_{\mathrm{SK}}^2)\,(\vec{o} \cdot \vec{o}')  t_{pp2} - [\vec{o} \cdot (\vec{r}/r_{\mathrm{SK}})] [\vec{o}' \cdot (\vec{r}/r_{\mathrm{SK}})] (t_{pp1} + t_{pp2}), & o,o' \in \{p_x, p_y, p_z\}
#  \end{cases} $$
#  
# where $o$ and $o'$ are the orbital types of the 1st and 2nd atoms and $\vec{r}$ is a vector pointing from the 2nd atom to the 1st atom. We are assigning direction vectors to the p orbitals, $\vec{p}_x \equiv (1,0,0)$, $\vec{p}_y \equiv (0,1,0)$, and $\vec{p}_z \equiv (0,0,1)$, to simplify the notation. This project has multiple case-based formulas, and we will implement them using a code structure similar to each formula:

# In[33]:


vec = { 'px':[1,0,0], 'py':[0,1,0], 'pz':[0,0,1] }

def hopping_energy(o1, o2, r12, model_parameters):
    '''Returns the hopping matrix element for a pair of orbitals of type o1 & o2 separated by a vector r12.'''
    r12_rescaled = r12 / model_parameters['r_hop']
    r12_length = np.linalg.norm(r12_rescaled)
    ans = np.exp( 1.0 - r12_length**2 )
    if o1 == 's' and o2 == 's':
        ans *= model_parameters['t_ss']
    if o1 == 's' and o2 in p_orbitals:
        ans *= np.dot(vec[o2], r12_rescaled) * model_parameters['t_sp']
    if o2 == 's' and o1 in p_orbitals:
        ans *= -np.dot(vec[o1], r12_rescaled)* model_parameters['t_sp']
    if o1 in p_orbitals and o2 in p_orbitals:
        ans *= ( (r12_length**2) * np.dot(vec[o1], vec[o2]) * model_parameters['t_pp2']
                 - np.dot(vec[o1], r12_rescaled) * np.dot(vec[o2], r12_rescaled)
                 * ( model_parameters['t_pp1'] + model_parameters['t_pp2'] ) )
    return ans

hop_vector = np.array([-model_parameters['r_hop'],0,0])
print('hopping tests:')
print('s s =', hopping_energy('s','s',hop_vector,model_parameters), model_parameters['t_ss'])
print('s px =', hopping_energy('s','px',hop_vector,model_parameters), model_parameters['t_sp'])
print('px px =', hopping_energy('px','px',hop_vector,model_parameters), model_parameters['t_pp1'])
print('s py =', hopping_energy('s','py',hop_vector,model_parameters))
print('py px =', hopping_energy('py','px',hop_vector,model_parameters))
print('py py =', hopping_energy('py','py',hop_vector,model_parameters), model_parameters['t_pp2'])


# ### C. Coulomb interaction
# 
# For the purpose of electrostatics, we will describe all inter-atomic Coulomb interactions with point charges. We need access to both the interaction kernel and its derivatives to define the multipole expansion,
# 
# $$ V_{o,o'}(\vec{r}) = \begin{cases}
#  1/r , & o = o' = s \\
#  (\vec{o}' \cdot \vec{r}) / r^3, & o = s \ \& \ o' \in \{p_x, p_y, p_z\} \\
#  -(\vec{o} \cdot \vec{r}) / r^3 , & o' = s \ \& \ o \in \{p_x, p_y, p_z\} \\
#  (\vec{o} \cdot \vec{o}') / r^3 - 3 (\vec{o} \cdot \vec{r}) (\vec{o}' \cdot \vec{r}) / r^5, & o,o' \in \{p_x, p_y, p_z\}
#  \end{cases} $$
# 
# The Coulomb energy has the same case-based structure and angular dependences as the hopping energy:

# In[38]:


def coulomb_energy(o1, o2, r12):
    '''Returns the Coulomb matrix element for a pair of multipoles of type o1 & o2 separated by a vector r12.'''
    r12_length = np.linalg.norm(r12)
    if o1 == 's' and o2 == 's':
        ans = 1.0 / r12_length
    if o1 == 's' and o2 in p_orbitals:
        ans = np.dot(vec[o2], r12) / r12_length**3
    if o2 == 's' and o1 in p_orbitals:
        ans = -np.dot(vec[o1], r12) / r12_length**3
    if o1 in p_orbitals and o2 in p_orbitals:
        ans = ( np.dot(vec[o1], vec[o2]) / r12_length**3
               - 3.0 * np.dot(vec[o1], r12) * np.dot(vec[o2], r12) / r12_length**5 )
    return ans 

coulomb_vector = np.array([-1,0,0])
print('Coulomb tests:')
print('s s =', coulomb_energy('s','s',coulomb_vector))
print('s px =', coulomb_energy('s','px',coulomb_vector))
print('px px =', coulomb_energy('px','px',coulomb_vector))
print('s py =', coulomb_energy('s','py',coulomb_vector))
print('py px =', coulomb_energy('py','px',coulomb_vector))
print('py py =', coulomb_energy('py','py',coulomb_vector))


# The semiempirical model approximations strongly distort the physics of inter-atomic Pauli repulsion, and we compensate for these errors with a short-range ionic pseudopotential, which is a common tool in physics for building effective models of ionic cores:
# 
# $$ V_o^{\mathrm{pseudo}}(\vec{r}) = V_{\mathrm{pseudo}} \exp(1 - r^2/r_{\mathrm{pseudo}}^2) \times \begin{cases}
# 1, & o = s \\
# -2 \vec{o}\cdot\vec{r}/r_{\mathrm{pseudo}}, & o \in \{p_x, p_y, p_z\} \end{cases} . $$
# 
# Note that the vector here points from the atom that the pseudopotential is centered on to the orbital that is interacting with it:

# In[41]:


def pseudopotential_energy(o, r, model_parameters):
    '''Returns the energy of a pseudopotential between a multipole of type o and an atom separated by a vector r.'''
    ans = model_parameters['v_pseudo']
    r_rescaled = r / model_parameters['r_pseudo']
    r_length = np.linalg.norm(r_rescaled)
    ans *= np.exp( 1.0 - r_length**2 )
    if o in p_orbitals:
        ans *= -2.0 * np.dot(vec[o], r_rescaled)
    return ans

pseudo_vector = np.array([-model_parameters['r_pseudo'],0,0])
print('pseudopotential tests:')
print('s =', pseudopotential_energy('s',pseudo_vector, model_parameters), model_parameters['v_pseudo'])
print('px =', pseudopotential_energy('px',pseudo_vector, model_parameters))
print('py =', pseudopotential_energy('py',pseudo_vector, model_parameters))


# These interaction kernels enable us to define and calculate the ion-ion energy in $\hat{H}$,
# 
# $$ E_{\mathrm{ion}} = Z^2 \sum_{i < j} V_{s,s}(\vec{r}_i - \vec{r}_j) $$

# In[43]:


def calculate_energy_ion(atomic_coordinates):
    '''Returns the ionic contribution to the total energy for an input list of atomic coordinates.'''
    energy_ion = 0.0
    for i, r_i in enumerate(atomic_coordinates):
        for j, r_j in enumerate(atomic_coordinates):
            if i < j:
                energy_ion += (ionic_charge**2)*coulomb_energy('s', 's', r_i - r_j)
    return energy_ion

energy_ion = calculate_energy_ion(atomic_coordinates)
print('E_ion =', energy_ion)


# the vector of electron-ion interactions (omitting on-site Coulomb terms),
# 
# $$ V^{\mathrm{ion}}_p = \sum_{i \neq \mathrm{atom}(p)}
#    V_{\mathrm{orb}(p)}^{\mathrm{pseudo}}(\vec{r}_{\mathrm{atom}(p)} - \vec{r}_i)
#    - Z V_{\mathrm{orb}(p),s}(\vec{r}_{\mathrm{atom}(p)} - \vec{r}_i)$$

# In[45]:


def calculate_potential_vector(atomic_coordinates, model_parameters):
    '''Returns the electron-ion potential energy vector for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*orbitals_per_atom
    potential_vector = np.zeros(ndof)
    for p in range(ndof):
        potential_vector[p] = 0.0
        for atom_i,r_i in enumerate(atomic_coordinates):
            r_pi = atomic_coordinates[atom(p)] - r_i
            if atom_i != atom(p):
                potential_vector[p] += ( pseudopotential_energy(orb(p), r_pi, model_parameters) 
                                         - ionic_charge * coulomb_energy(orb(p), 's', r_pi) )
    return potential_vector

potential_vector = calculate_potential_vector(atomic_coordinates, model_parameters)
np.set_printoptions(precision=1)
print('V_ion =', potential_vector)


# and the matrix of electron-electron interaction matrix elements,
# 
# $$ V^{\mathrm{ee}}_{p,q} = \begin{cases}
#  V_{\mathrm{orb}(p),\mathrm{orb}(q)}(\vec{r}_{\mathrm{atom}(p)} - \vec{r}_{\mathrm{atom}(q)}) , & \mathrm{atom}(p) \neq \mathrm{atom}(q) \\
#  V^{\mathrm{self}}_{\mathrm{orb}(p)} \delta_{p,q} , & \mathrm{atom}(p) = \mathrm{atom}(q)
# \end{cases} . $$
# 
# On-site Coulomb interactions between electrons are described by electronic self-energy parameters $V_o^{\mathrm{self}}$ for multipole moment $o$.

# In[46]:


def calculate_interaction_matrix(atomic_coordinates, model_parameters):
    '''Returns the electron-electron interaction energy matrix for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*orbitals_per_atom
    interaction_matrix = np.zeros( (ndof,ndof) )
    for p in range(ndof):
        for q in range(ndof):
            if atom(p) != atom(q):
                r_pq = atomic_coordinates[atom(p)] - atomic_coordinates[atom(q)]
                interaction_matrix[p,q] = coulomb_energy(orb(p), orb(q), r_pq)
            if p == q and orb(p) == 's':
                interaction_matrix[p,q] = model_parameters['coulomb_s']
            if p == q and orb(p) in p_orbitals:
                interaction_matrix[p,q] = model_parameters['coulomb_p']                
    return interaction_matrix

interaction_matrix = calculate_interaction_matrix(atomic_coordinates, model_parameters)
print('V_ee =\n', interaction_matrix)


# ### D. Multipole decomposition
# 
# To define $V_{p,q,r,s}$ based on the Coulomb matrix elements $V_{p,q}^{\mathrm{ee}}$, we need to define a mapping from products of atomic orbitals to a linear combination of terms in the multipole expansion of electronic charge. Because the atomic orbitals are normalized, their monopole coefficient with themselves is 1 (i.e. they have unit charge). Because of orbital orthogonality, there is no monopole term for either intra-atomic or inter-atomic transitions between atomic orbitals. We will ignore dipole transitions between atoms, which corresponds to the [neglect of diatomic differential overlap](https://en.wikipedia.org/wiki/NDDO) (NDDO) approximation that is commonly used in semiempirical quantum chemistry. All that remains is the intra-atomic s-p transition, and we will use a model parameter, $D$, to define its dipole strength. These transformation rules between atomic orbitals and multipole moments can be summarized pictorially,
# 
# ![s-p multipole diagram](multipole_cases.png)
# 
# or mathematically as a 3-index tensor $\chi_{p,q,r}$ where $p$ and $q$ are the atomic orbital indices and $r$ is the multipole moment index,
# 
# $$ \chi_{p, q, r} = \begin{cases} 1, & \mathrm{orb}(p) = \mathrm{orb}(q) \ \& \ \mathrm{orb}(r) = s \ \& \ \mathrm{atom}(p) = \mathrm{atom}(q) = \mathrm{atom}(r) \\ 
#  D , & \mathrm{orb}(q) = \mathrm{orb}(r) \in \{p_x, p_y, p_z\} \ \& \ \mathrm{orb}(p) = s \ \& \ \mathrm{atom}(p) = \mathrm{atom}(q) = \mathrm{atom}(r) \\
#  D , & \mathrm{orb}(p) = \mathrm{orb}(r) \in \{p_x, p_y, p_z\} \ \& \ \mathrm{orb}(q) = s \ \& \ \mathrm{atom}(p) = \mathrm{atom}(q) = \mathrm{atom}(r) \\
#  0, & \mathrm{otherwise} \end{cases} . $$

# In[47]:


def chi_on_atom(o1, o2, o3, model_parameters):
    '''Returns the value of the chi tensor for 3 orbital indices on the same atom.'''
    if o1 == o2 and o3 == 's':
        return 1.0
    if o1 == o3 and o3 in p_orbitals and o2 == 's':
        return model_parameters['dipole']
    if o2 == o3 and o3 in p_orbitals and o1 == 's':
        return model_parameters['dipole']
    return 0.0

def calculate_chi_tensor(atomic_coordinates, model_parameters):
    '''Returns the chi tensor for an input list of atomic coordinates'''
    ndof = len(atomic_coordinates)*orbitals_per_atom
    chi_tensor = np.zeros( (ndof,ndof,ndof) )
    for p in range(ndof):
        for orb_q in orbital_types:
            q = ao_index(atom(p), orb_q) # p & q on same atom
            for orb_r in orbital_types:
                r = ao_index(atom(p), orb_r) # p & r on same atom
                chi_tensor[p,q,r] = chi_on_atom(orb(p), orb(q), orb(r), model_parameters)
    return chi_tensor

chi_tensor = calculate_chi_tensor(atomic_coordinates, model_parameters)
print('chi =\n',chi_tensor)


# The multipole expansion plays the same role as an auxiliary basis set in modern resolution-of-identity (RI) methods that are used to accelerate large quantum chemistry simulations. The primary purpose of these methods is to decompose $V_{p,q,r,s}$ into a low-rank factored form, which is
# 
# $$ V_{p,q,r,s} = \sum_{t,u} \chi_{p,q,t} V_{t,u}^{\mathrm{ee}} \chi_{r,s,u} $$
# 
# in our semiempirical model. Unlike the previous vectors and matrices that we have constructed, the $\chi$ and $V$ tensors are sparse, meaning that most of their entries are zero. Without a more clever implementation, it can be computationally slow and wasteful to store and compute with these zero values. Modern quantum chemistry methods contain many ways of identifying and utilizing sparsity. For example, we could compute tensor elements of $\chi$ on-the-fly rather than storing the full tensor. This is analogous to "integral direct" methods in quantum chemistry. For simplicity, we will still precompute and store $\chi$, which fails to utilize its sparsity. However, we will avoid the explicit construction of $V_{p,q,r,s}$ by utilizing its factored form.
# 
# ### E. 1-body Hamiltonian
# 
# The 1-body Hamiltonian coefficients $h_{p,q}$ combine many of the components that we have already discussed and implemented along with the on-site orbital energies, $E_s$ and $E_p$, which are the final two parameters from the semiempirical model,
# 
# $$ h_{p,q} = \begin{cases}
#  t_{\mathrm{orb}(p),\mathrm{orb}(q)}(\vec{r}_{\mathrm{atom}(p)} - \vec{r}_{\mathrm{atom}(q)})
#   , & \mathrm{atom}(p) \neq \mathrm{atom}(q) \\
#  E_{\mathrm{orb}(p)} \delta_{\mathrm{orb}(p),\mathrm{orb}(q)} + \sum_{r} \chi_{p,q,r} V_{r}^{\mathrm{ion}} , & \mathrm{atom}(p) = \mathrm{atom}(q) 
#   \end{cases}. $$

# In[53]:


def calculate_hamiltonian_matrix(atomic_coordinates, model_parameters):
    '''Returns the 1-body Hamiltonian matrix for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*orbitals_per_atom
    hamiltonian_matrix = np.zeros( (ndof,ndof) )
    potential_vector = calculate_potential_vector(atomic_coordinates, model_parameters)
    for p in range(ndof):
        for q in range(ndof):
            if atom(p) != atom(q):
                r_pq = atomic_coordinates[atom(p)] - atomic_coordinates[atom(q)]
                hamiltonian_matrix[p,q] = hopping_energy(orb(p), orb(q), r_pq, model_parameters)
            if atom(p) == atom(q):
                if p == q and orb(p) == 's':
                    hamiltonian_matrix[p,q] += model_parameters['energy_s']
                if p == q and orb(p) in p_orbitals:
                    hamiltonian_matrix[p,q] += model_parameters['energy_p']
                for orb_r in orbital_types:
                    r = ao_index(atom(p), orb_r)
                    hamiltonian_matrix[p,q] += ( chi_on_atom(orb(p), orb(q), orb_r, model_parameters)
                                                 * potential_vector[r] )
    return hamiltonian_matrix

hamiltonian_matrix = calculate_hamiltonian_matrix(atomic_coordinates, model_parameters)
print('hamiltonian matrix =\n',hamiltonian_matrix)


# We have now fully specified the many-body Hamiltonian $\hat{H}$, and we can now move on to approximating and calculating some of its physical properties.
# 
# ## 3. Hartree-Fock theory
# 
# Even with a simple model for $\hat{H}$, we cannot calculate its ground state energy exactly for more than a few Argon atoms. Instead, we will use the [Hartree-Fock approximation](https://en.wikipedia.org/wiki/Hartree–Fock_method), which restricts the ground-state wavefunction to a single Slater determinant. We will find the Slater determinant with the lowest total energy, but a more general wavefunction will usually have an even lower energy. In the next section, we will use many-body perturbation theory to improve our estimate of the total energy.
# 
# The central objects of Hartree-Fock theory are the 1-electron density matrix $\rho_{p,q}$ and the Fock matrix $f_{p,q}$. These two matrices depend on each other, which defines a nonlinear set of equations that we must solve iteratively. Iteratively solving these equations is usually referred to as the self-consistent field (SCF) cycle. For this to converge, we must start from a reasonable initial guess for $\rho_{p,q}$. We will initialize it to the density matrix for isolated Argon atoms,
# 
# $$ \rho_{p,q}^{\mathrm{atom}} = \begin{cases} 
#    1, & p = q \ \& \ \mathrm{orb}(p) \in \{ p_x, p_y, p_z \} \\
#    0, & \mathrm{otherwise}
# \end{cases} $$

# In[54]:


orbital_occupation = { 's':0, 'px':1, 'py':1, 'pz':1 }

def calculate_atomic_density_matrix(atomic_coordinates):
    '''Returns a trial 1-electron density matrix for an input list of atomic coordinates.'''
    ndof = len(atomic_coordinates)*orbitals_per_atom
    density_matrix = np.zeros( (ndof,ndof) )
    for p in range(ndof):
        density_matrix[p,p] = orbital_occupation[orb(p)]
    return density_matrix

density_matrix = calculate_atomic_density_matrix(atomic_coordinates)
print('atomic density matrix =\n', density_matrix)


# Because of spin symmetry, we use the same $\rho_{p,q}$ for each electron spin type, which reduces the sum over spin types in $\hat{H}$ to degeneracy pre-factors. Thus, half of the electrons are spin-up and half are spin-down.
# 
# The Fock matrix is defined by the density matrix,
# 
# $$ \begin{align} f_{p,q} & = h_{p,q} + \sum_{r,s} ( 2 V_{p,q,r,s} - V_{r,q,p,s} )\rho_{r,s} \\
#                          & = h_{p,q} + \sum_{r,s,t,u} ( 2 \chi_{p,q,t} \chi_{r,s,u} - \chi_{r,q,t} \chi_{p,s,u} )
#                              V_{t,u}^{\mathrm{ee}} \rho_{r,s} ,
#    \end{align} $$
# 
# which is just a sum of two tensor contractions between the $V$ tensor and the density matrix. Because the $V$ tensor has low rank, we can save a lot of computational effort by using its factored form to construct the Fock matrix:

# In[55]:


def calculate_fock_matrix(hamiltonian_matrix, interaction_matrix, density_matrix, chi_tensor):
    '''Returns the Fock matrix defined by the input Hamiltonian, interaction, & density matrices.'''
    fock_matrix = hamiltonian_matrix.copy()
    fock_matrix += 2.0*np.einsum('pqt,rsu,tu,rs',
                                 chi_tensor, chi_tensor, interaction_matrix, density_matrix, optimize=True)
    fock_matrix -= np.einsum('rqt,psu,tu,rs',
                             chi_tensor, chi_tensor, interaction_matrix, density_matrix, optimize=True)
    return fock_matrix

fock_matrix = calculate_fock_matrix(hamiltonian_matrix, interaction_matrix, density_matrix, chi_tensor)
print('atomic fock matrix =\n', fock_matrix)


# This is our first example of using "einsum" in NumPy to trivialize the implementation of a tensor equation using the [Einstein summation convention](https://en.wikipedia.org/wiki/Einstein_notation) and some extra conventions for the order of indices (alphabetical order).
# 
# The Fock matrix defines the matrix of molecular orbitals $\phi_{i,j}$ as its eigenvectors,
# 
# $$ \sum_{q} f_{p,q} \phi_{q,r} = E_r \phi_{p,r}, $$
# 
# and the density matrix is defined by the occupied orbitals (we occupy the lowest-energy orbitals with all of the electrons available),
# 
# $$ \rho_{p,q} = \sum_{i \in \mathrm{occ}} \phi_{p,i} \phi_{q,i}. $$

# In[56]:


def calculate_density_matrix(fock_matrix):
    '''Returns the 1-electron density matrix defined by the input Fock matrix.'''
    num_occ = (ionic_charge//2)*np.size(fock_matrix,0)//orbitals_per_atom
    orbital_energy, orbital_matrix = np.linalg.eigh(fock_matrix)
    occupied_matrix = orbital_matrix[:,:num_occ]
    density_matrix = occupied_matrix @ occupied_matrix.T
    return density_matrix

density_matrix = calculate_density_matrix(fock_matrix)
print('corrected density matrix =\n', density_matrix)


# To calculate a $\rho_{p,q}$ and $f_{p,q}$ that are consistent with each other, we mix in a fraction of the new density matrix with the old density matrix and re-calculate them until convergence. A very simple pseudocode for one loop of this cycle is
# 
# $$ \begin{align} f_{\mathrm{new}} &= f(\rho_{\mathrm{old}}) \\
#                  \rho_{\mathrm{new}} &= w_{\mathrm{mix}} \rho(f_{\mathrm{new}})
#                  + (1 - w_{\mathrm{mix}}) \rho_{\mathrm{old}} \end{align} $$
# 
# This is a crude strategy for an SCF cycle, but it works for weakly interacting atoms like Argon:

# In[64]:


def scf_cycle(hamiltonian_matrix, interaction_matrix, density_matrix, chi_tensor,
              max_scf_iterations = 100, mixing_fraction = 0.25, convergence_tolerance = 1e-4):
    '''Returns converged density & Fock matrices defined by the input Hamiltonian, interaction, & density matrices.'''
    old_density_matrix = density_matrix.copy()
    for iteration in range(max_scf_iterations):
        new_fock_matrix = calculate_fock_matrix(hamiltonian_matrix, interaction_matrix, old_density_matrix, chi_tensor)
        new_density_matrix = calculate_density_matrix(new_fock_matrix)

        error_norm = np.linalg.norm( old_density_matrix - new_density_matrix )
        if error_norm < convergence_tolerance:
            return new_density_matrix, new_fock_matrix

        old_density_matrix = (mixing_fraction * new_density_matrix
                              + (1.0 - mixing_fraction) * old_density_matrix)
    print("WARNING: SCF cycle didn't converge")
    return new_density_matrix, new_fock_matrix

density_matrix, fock_matrix = scf_cycle(hamiltonian_matrix, interaction_matrix, density_matrix, chi_tensor)
print('SCF density matrix:\n',density_matrix)


# Once we have a converged density matrix, the Hartree-Fock total energy is defined as
# 
# $$ \begin{align}
#   E_{\mathrm{HF}} &= E_{\mathrm{ion}} + E_{\mathrm{SCF}} \\
#   E_{\mathrm{SCF}} &= 2 \sum_{p,q} h_{p,q} \rho_{p,q}
#   + \sum_{p,q,r,s} V_{p,q,r,s} ( 2 \rho_{p,q} \rho_{r,s} - \rho_{p,s} \rho_{r,q} ) = \sum_{p,q} (h_{p,q} + f_{p,q}) \rho_{p,q}
#   \end{align} $$

# In[66]:


def calculate_energy_scf(hamiltonian_matrix, fock_matrix, density_matrix):
    '''Returns the Hartree-Fock total energy defined by the input Hamiltonian, Fock, & density matrices.'''
    energy_scf = np.einsum('pq,pq',hamiltonian_matrix + fock_matrix,density_matrix)
    return energy_scf

energy_scf = calculate_energy_scf(hamiltonian_matrix, fock_matrix, density_matrix)
print('Hartree-Fock energy =',energy_ion + energy_scf)


# The bottleneck of this Hartree-Fock implementation is the tensor contraction in the formation of the Fock matrix. Because we are not exploiting the sparsity of $\chi$, it scales as $O(n^4)$ operations for $n$ atoms because of 3 free tensor indices and 1 summed tensor index in the contractions with the dense $\chi$ tensor.
# 
# ## 4. 2nd-order Moller-Plesset (MP2) perturbation theory
# 
# The Hartree-Fock approximation does not describe the London dispersion interaction between Argon atoms. We must use many-body perturbation theory to improve our approximation to the ground state of $\hat{H}$ and include this physical effect. Thankfully, we only need to use the first correction beyond Hartree-Fock theory, which is [second-order Moller-Plesset (MP2) perturbation theory](https://en.wikipedia.org/wiki/Møller–Plesset_perturbation_theory). It is simple, but computationally expensive. The computational bottleneck is the transformation of the tensor $V_{p,q,r,s}$ from the atomic orbital basis to the molecular orbital basis,
# 
# $$ \tilde{V}_{a,i,b,j} = \sum_{p,q,r,s} \phi_{p,a} \phi_{q,i} \phi_{r,b} \phi_{s,j} V_{p,q,r,s}. $$
# 
# where we are restricting some indices to occupied orbitals ($i$ and $j$) and some indices to the unoccupied "virtual" orbitals ($a$ and $b$). This use of specific orbital labels to denote a restriction of indices is standard notation in quantum chemistry. To make this easier to program, we can partition the molecular orbitals and their energies into occupied and virtual:

# In[67]:


def partition_orbitals(fock_matrix):
    '''Returns a list with the occupied/virtual energies & orbitals defined by the input Fock matrix.'''
    num_occ = (ionic_charge//2)*np.size(fock_matrix,0)//orbitals_per_atom
    orbital_energy, orbital_matrix = np.linalg.eigh(fock_matrix)
    occupied_energy = orbital_energy[:num_occ]
    virtual_energy = orbital_energy[num_occ:]
    occupied_matrix = orbital_matrix[:,:num_occ]
    virtual_matrix = orbital_matrix[:,num_occ:]

    return occupied_energy, virtual_energy, occupied_matrix, virtual_matrix

occupied_energy, virtual_energy, occupied_matrix, virtual_matrix = partition_orbitals(fock_matrix)

print('occupied orbital energies:\n', occupied_energy)
print('virtual orbital energies:\n', virtual_energy)


# We use the factored form of $V_{p,q,r,s}$ to reduce our intermediate memory usage, but we do need to explicitly store $\tilde{V}_{a,i,b,j}$,
# 
# $$ \begin{align}
#    \tilde{\chi}_{a,i,p} &= \sum_{q,r} \phi_{q,a} \phi_{r,i} \chi_{q,r,p} \\
#    \tilde{V}_{a,i,b,j} &= \sum_{p,q} \tilde{\chi}_{a,i,p} V_{p,q}^{\mathrm{ee}} \tilde{\chi}_{b,j,q}
#    \end{align} $$

# In[68]:


def transform_interaction_tensor(occupied_matrix, virtual_matrix, interaction_matrix, chi_tensor):
    '''Returns a transformed V tensor defined by the input occupied, virtual, & interaction matrices.'''
    chi2_tensor = np.einsum('qa,ri,qrp', virtual_matrix, occupied_matrix, chi_tensor, optimize=True)
    interaction_tensor = np.einsum('aip,pq,bjq->aibj', chi2_tensor, interaction_matrix, chi2_tensor, optimize=True)
    return interaction_tensor

interaction_tensor = transform_interaction_tensor(occupied_matrix, virtual_matrix, interaction_matrix, chi_tensor)
print('interaction tensor =\n', interaction_tensor)


# In this simple implementation, we are not taking full advantage of the sparisty of the initial $\chi$ tensor. Unlike the $V$ tensor, the $\tilde{V}$ tensor is constructed explicitly and stored in memory. The MP2 correction to the total energy is then just a large summation over previously computed and stored quantities,
# 
# $$ E_{\mathrm{MP2}} = - \sum_{a,b \in \mathrm{virt}} \sum_{i,j \in \mathrm{occ}} \frac{2 \tilde{V}_{a,i,b,j}^2 - \tilde{V}_{a,i,b,j} \tilde{V}_{a,j,b,i}}{E_a + E_b - E_i - E_j} . $$

# In[21]:


def calculate_energy_mp2(fock_matrix, interaction_matrix, chi_tensor):
    '''Returns the MP2 contribution to the total energy defined by the input Fock & interaction matrices.'''
    E_occ, E_virt, occupied_matrix, virtual_matrix = partition_orbitals(fock_matrix)
    V_tilde = transform_interaction_tensor(occupied_matrix, virtual_matrix, interaction_matrix, chi_tensor)

    energy_mp2 = 0.0
    num_occ = len(E_occ)
    num_virt = len(E_virt)
    for a in range(num_virt):
        for b in range(num_virt):
            for i in range(num_occ):
                for j in range(num_occ):
                    energy_mp2 -= ( (2.0*V_tilde[a,i,b,j]**2 - V_tilde[a,i,b,j]*V_tilde[a,j,b,i])
                                    /(E_virt[a] + E_virt[b] - E_occ[i] - E_occ[j]) )
    return energy_mp2

energy_mp2 = calculate_energy_mp2(fock_matrix, interaction_matrix, chi_tensor)
print(energy_mp2)


# The bottleneck of MP2 calculations is the formation of $\tilde{V}$ from $\tilde{\chi}$ and $V^{\mathrm{ee}}$, which scales as $O(n^5)$ operations for $n$ atoms because of 4 free tensor indices and 1 summed tensor index once the contraction has been optimally arranged into intermediate operations. The 4 nested for loops that accumulate the MP2 energy correction have a lower $O(n^4)$ scaling of cost but with a very large prefactor that will usually cause it to dominate the overall cost.
# 
# ## (Extra Credit) 5. Alternative Fock matrix construction
# 
# The previous implementation of Fock matrix construction,
# 
# $$ f_{p,q} = h_{p,q} + \sum_{r,s,t,u} ( 2 \chi_{p,q,t} \chi_{r,s,u} - \chi_{r,q,t} \chi_{p,s,u} )
#                              V_{t,u}^{\mathrm{ee}} \rho_{r,s}, $$
# 
# did not utilize the sparsity of the $\chi$ tensor. As a result, its computational cost scaling is not as low as it could be, and it will have poor performance for large system sizes. We can improve the scaling by making use of the fact that $\chi_{p,q,r}$ is only non-zero when all of its indices are on the same atom:

# In[69]:


def calculate_fock_matrix_fast(hamiltonian_matrix, interaction_matrix, density_matrix, model_parameters):
    '''Returns the Fock matrix defined by the input Hamiltonian, interaction, & density matrices.'''
    ndof = np.size(hamiltonian_matrix,0)
    fock_matrix = hamiltonian_matrix.copy()
    # Hartree potential term
    for p in range(ndof):
        for orb_q in orbital_types:
            q = ao_index(atom(p), orb_q) # p & q on same atom
            for orb_t in orbital_types:
                t = ao_index(atom(p), orb_t) # p & t on same atom
                chi_pqt = chi_on_atom(orb(p), orb_q, orb_t, model_parameters)
                for r in range(ndof):
                    for orb_s in orbital_types:
                        s = ao_index(atom(r), orb_s) # r & s on same atom
                        for orb_u in orbital_types:
                            u = ao_index(atom(r), orb_u) # r & u on same atom
                            chi_rsu = chi_on_atom(orb(r), orb_s, orb_u, model_parameters)
                            fock_matrix[p,q] += 2.0 * chi_pqt * chi_rsu * interaction_matrix[t,u] * density_matrix[r,s]
    # Fock exchange term
    for p in range(ndof):
        for orb_s in orbital_types:
            s = ao_index(atom(p), orb_s) # p & s on same atom
            for orb_u in orbital_types:
                u = ao_index(atom(p), orb_u) # p & u on same atom
                chi_psu = chi_on_atom(orb(p), orb_s, orb_u, model_parameters)
                for q in range(ndof):
                    for orb_r in orbital_types:
                        r = ao_index(atom(q), orb_r) # q & r on same atom
                        for orb_t in orbital_types:
                            t = ao_index(atom(q), orb_t) # q & t on same atom
                            chi_rqt = chi_on_atom(orb_r, orb(q), orb_t, model_parameters)
                            fock_matrix[p,q] -= chi_rqt * chi_psu * interaction_matrix[t,u] * density_matrix[r,s]
    return fock_matrix

fock_matrix1 = calculate_fock_matrix(hamiltonian_matrix, interaction_matrix, density_matrix, chi_tensor)
fock_matrix2 = calculate_fock_matrix_fast(hamiltonian_matrix, interaction_matrix, density_matrix, model_parameters)
print('difference between fock matrix implementations =', np.linalg.norm(fock_matrix1 - fock_matrix2))


# This algorithm is much more complicated to implement and it is doing something that should be avoided at all costs in Python: deeply nested for loops. While this algorithm has good scaling, we will soon observe a very large cost prefactor in its performance. We can compare the performance of these algorithms by setting up a sequence of Argon clusters of increasing size:

# In[70]:


from timeit import default_timer as timer

def build_fcc_cluster(radius, lattice_constant):
    vec1 = np.array([lattice_constant, lattice_constant, 0.0])
    vec2 = np.array([lattice_constant, 0.0, lattice_constant])
    vec3 = np.array([0.0, lattice_constant, lattice_constant])
    max_index = int(radius/np.linalg.norm(vec1))
    atomic_coordinates = np.array([ i*vec1 + j*vec2 + k*vec3 for i in range(-max_index, max_index+1)
                                                             for j in range(-max_index, max_index+1)
                                                             for k in range(-max_index, max_index+1)
                                                             if np.linalg.norm(i*vec1 + j*vec2 + k*vec3) <= radius ])
    return atomic_coordinates

for radius in np.arange(10.0,40.0,10.0):
    coord = build_fcc_cluster(radius, 9.9)
    
    time1 = timer()
    density_matrix = calculate_atomic_density_matrix(coord)
    hamiltonian_matrix = calculate_hamiltonian_matrix(coord, model_parameters)
    interaction_matrix = calculate_interaction_matrix(coord, model_parameters)
    chi_tensor = calculate_chi_tensor(coord, model_parameters)
    time2 = timer()
    print(len(coord))
    print('base matrix construction time =',time2-time1)
    fock_matrix = calculate_fock_matrix(hamiltonian_matrix, interaction_matrix, density_matrix, chi_tensor)
    time3 = timer()
    print('Fock matrix construction time ("slow") =',time3-time2)
    fock_matrix = calculate_fock_matrix_fast(hamiltonian_matrix, interaction_matrix, density_matrix, model_parameters)
    time4 = timer()
    print('Fock matrix construction time ("fast") =',time4-time3)


# We could improve the implementation of Fock matrix construction in Python by organizing much of the work as dense matrix-matrix multiplications on small blocks of the matrix, which involves more sophisticated bookkeeping. Another way to improve its implementation is simply to port it as-is to C or C++, which have very low performance overheads in nested for loops. Thus an implementation that superficially looks very similar can have dramatically improved performance.
# 
# ## (Extra Credit) 6. Semiempirical model parameterization
# 
# For a semiempirical model to be useful, it must be parameterized to fit a set of reference data, usually from experiments or high-accuracy quantum chemistry simulation data. The parameters in this project have already been fit to data, which was carried out using the fitting procedure implemented in this section. Data fitting is an optimization problem, and we can make use of pre-existing Python optimization tools by encapsulating the fitting process into a function that inputs a list of parameters and outputs a fitting error that we want to minimize. We begin with our reference data, which is the set of occupied orbital energies, Hartree-Fock binding energies, and MP2 binding energies from all-electron calculations of Argon dimers at various separation distances:

# In[24]:


# distance, HF binding energy, MP2 binding energy, 6 occupied HF orbital energies
reference_dimer = [ [ 5.67, 0.0064861, -0.0037137, -0.620241, -0.595697, -0.595697, -0.584812, -0.584812, -0.561277 ],
                    [ 6.24, 0.0022351, -0.0020972, -0.608293, -0.593456, -0.593456, -0.587742, -0.587742, -0.573604 ],
                    [ 6.62, 0.0010879, -0.0014491, -0.603106, -0.592621, -0.592621, -0.588897, -0.588897, -0.578874 ],
                    [ 6.99, 0.0005255, -0.0010120, -0.599442, -0.592082, -0.592082, -0.589651, -0.589651, -0.582576 ],
                    [ 7.18, 0.0003642, -0.0008494, -0.598041, -0.591886, -0.591886, -0.589922, -0.589922, -0.583988 ],
                    [ 7.56, 0.0001737, -0.0006037, -0.595889, -0.591599, -0.591599, -0.590315, -0.590315, -0.586154 ],
                    [ 7.94, 0.0000818, -0.0004342, -0.594388, -0.591408, -0.591408, -0.590569, -0.590569, -0.587662 ],
                    [ 8.32, 0.0000377, -0.0003159, -0.593344, -0.591281, -0.591281, -0.590731, -0.590731, -0.588708 ],
                    [ 9.26, 0.0000047, -0.0001502, -0.591934, -0.591118, -0.591118, -0.590927, -0.590927, -0.590120 ],
                    [ 10.21, 0.000000, -0.0000757, -0.591378, -0.591059, -0.591059, -0.590993, -0.590993, -0.590675 ] ]


# We then write a function that encapsulates all of the setup and solving from earlier in the notebook to generate model predictions of the reference data:

# In[25]:


def calculate_reference_data(atomic_coordinates, model_parameters):
    '''Returns the occupied orbital, HF, & MP2 energies for the input list of atomic coordinates'''
    ndof = len(atomic_coordinates)*orbitals_per_atom

    energy_ion = calculate_energy_ion(atomic_coordinates)
    density_matrix = calculate_atomic_density_matrix(atomic_coordinates)

    hamiltonian_matrix = calculate_hamiltonian_matrix(atomic_coordinates, model_parameters)
    interaction_matrix = calculate_interaction_matrix(atomic_coordinates, model_parameters)
    chi_tensor = calculate_chi_tensor(atomic_coordinates, model_parameters)

    density_matrix, fock_matrix = scf_cycle(hamiltonian_matrix, interaction_matrix, density_matrix, chi_tensor)

    occupied_energy, virtual_energy, occupied_matrix, virtual_matrix = partition_orbitals(fock_matrix)
    energy_scf = calculate_energy_scf(hamiltonian_matrix, fock_matrix, density_matrix)
    energy_hf = energy_ion + energy_scf
    energy_mp2 = calculate_energy_mp2(fock_matrix, interaction_matrix, chi_tensor)

    return occupied_energy, energy_hf, energy_mp2

print(calculate_reference_data(np.array([[0.0,0.0,0.0]]),model_parameters))
print(calculate_reference_data(np.array([[0.0,0.0,0.0],[6.99,0.0,0.0]]),model_parameters))
print(calculate_reference_data(np.array([[0.0,0.0,0.0],[20.0,0.0,0.0]]),model_parameters))


# We use this function inside yet another function that loops over the reference data, performs simulations on the reference geometries, and returns the root-mean-squared (RMS) deviation between the reference data and model predictions:

# In[72]:


def model_error(reference_dimer, model_parameters):
    '''Returns the RMS error between the model & reference data for the input set of model parameters'''
    atom_occupied, atom_hf, atom_mp2 = calculate_reference_data(np.array([[0,0,0]]), model_parameters)
    rms_error = 0.0

    for data_list in reference_dimer:
        mol_coord = np.array([ [0,0,0], [data_list[0],0,0] ])
        mol_occupied, mol_hf, mol_mp2 = calculate_reference_data(mol_coord, model_parameters)
        mol_hf -= 2.0*atom_hf
        mol_mp2 -= 2.0*atom_mp2
        rms_error += (mol_hf - data_list[1])**2 + (mol_mp2 - data_list[2])**2
        rms_error += np.linalg.norm(mol_occupied - data_list[3:])**2

#    print(np.sqrt(rms_error)/len(reference_dimer))
    return np.sqrt(rms_error)/len(reference_dimer)

print("rms error =", model_error(reference_dimer,model_parameters))


# Finally, we can use the SciPy optimizer to calculate the optimal model parameters:

# In[27]:


import scipy.optimize

def objective_wrapper(param_list, reference_dimer, model_parameters, lock):
    '''Wraps model_error so that the model parameters are in a list'''
    new_parameters = {}
    i = 0
    for key in sorted(model_parameters.keys()):
        if lock != None and key in lock:
            new_parameters.update( { key : model_parameters[key] } )
        else:
            new_parameters.update( { key : param_list[i] } )
            i += 1
    return model_error(reference_dimer, new_parameters)

def fit_model(reference_dimer, model_parameters, lock=None):
    '''Returns optimized model parameters that best fit the reference data'''

    param_list = []
    for key in sorted(model_parameters.keys()):
        if lock != None and key in lock:
            pass
        else:
            param_list.append( model_parameters[key] )
    min_opt = { 'maxiter' : 2000 }
    result = scipy.optimize.minimize(objective_wrapper, param_list,
                                     (reference_dimer, model_parameters, lock),
                                     method='Nelder-Mead', options=min_opt)

    opt_parameters = {}
    i = 0
    for key in sorted(model_parameters.keys()):
        if lock != None and key in lock:
            opt_parameters.update( { key : model_parameters[key] } )
        else:
            opt_parameters.update( { key : result.x[i] } )
            i += 1
    return opt_parameters

new_parameters = fit_model(reference_dimer, model_parameters)
print("new rms error =", model_error(reference_dimer,new_parameters))
print(new_parameters)


# Is this a good semiempirical model? Not really. It was designed to be simple in form and capture the correct qualitative behavior, but simplicity and accuracy are usually conflicting design principles. The model is not flexible enough to fit the data to very high accuracy, but its parameters also cannot be uniquely specified by the reference data because we aren't probing the system enough. We can identify this problem by examining the eigenvalues of the Hessian matrix for the objective function that we are minimizing to fit the model:

# In[ ]:


def model_hessian(reference_dimer, model_parameters, lock=None):
    param_list = []
    for key in sorted(model_parameters.keys()):
        if lock != None and key in lock:
            pass
        else:
            param_list.append( model_parameters[key] )
    num_param = len(param_list)
    hessian_matrix = np.zeros((num_param,num_param))
    dx = 0.01
    for i in range(num_param):
        for j in range(num_param):
            param_list[i] += dx
            param_list[j] += dx
            hes_pp = objective_wrapper(param_list, reference_dimer, model_parameters, lock)
            param_list[i] -= dx
            param_list[j] -= dx

            param_list[i] -= dx
            param_list[j] -= dx
            hes_mm = objective_wrapper(param_list, reference_dimer, model_parameters, lock)
            param_list[i] += dx
            param_list[j] += dx

            param_list[i] += dx
            param_list[j] -= dx
            hes_pm = objective_wrapper(param_list, reference_dimer, model_parameters, lock)
            param_list[i] -= dx
            param_list[j] += dx

            param_list[i] -= dx
            param_list[j] += dx
            hes_mp = objective_wrapper(param_list, reference_dimer, model_parameters, lock)
            param_list[i] += dx
            param_list[j] -= dx

            hessian_matrix[i,j] = (hes_pp - hes_mp - hes_pm + hes_mm)/(4*dx**2)
    return hessian_matrix

hessian_matrix = model_hessian(reference_dimer, new_parameters)
vals, vecs = np.linalg.eigh(hessian_matrix)
print('model Hessian eigenvalues:\n',vals)


# Small eigenvalues correspond to directions in parameter space where we can change parameters without changing how well the model fits the data. This problem is not specific to our simple QM project - semiempirical model building has persistent problems with model indeterminacy as they add more parameters and details to improve accuracy. Because these models have historically be used to reproduce a limited set of observables (e.g. heat of formation), they've been limited in what reference data they can use for model fitting.
# 
# We can use this same model and fitting process for other noble gases such as Neon by applying it to a different set of reference data:

# In[71]:


neon_ref_data = [
[ 5.29301 , 0.000356767 , -0.000275745 , -0.857733 , -0.851311 , -0.851311 , -0.849117 , -0.849117 , -0.843045 ],
[ 5.48204 , 0.000222991 , -0.000229153 , -0.856225 , -0.851119 , -0.851119 , -0.849443 , -0.849443 , -0.844581 ],
[ 5.67108 , 0.000138635 , -0.000190295 , -0.855025 , -0.850968 , -0.850968 , -0.849687 , -0.849687 , -0.845800 ],
[ 5.86011 , 8.54899e-05 , -0.000157984 , -0.854072 , -0.850849 , -0.850849 , -0.849869 , -0.849869 , -0.846765 ],
[ 6.04915 , 5.21333e-05 , -0.000131269 , -0.853315 , -0.850757 , -0.850757 , -0.850006 , -0.850006 , -0.847529 ],
[ 6.23819 , 3.13588e-05 , -0.000109318 , -0.852714 , -0.850684 , -0.850684 , -0.850108 , -0.850108 , -0.848135 ],
[ 6.42722 , 1.85686e-05 , -9.13551e-05 , -0.852237 , -0.850626 , -0.850626 , -0.850185 , -0.850185 , -0.848614 ],
[ 6.61626 , 1.07945e-05 , -7.66555e-05 , -0.851859 , -0.850582 , -0.850582 , -0.850244 , -0.850244 , -0.848993 ],
[ 6.80529 , 6.11193e-06 , -6.45702e-05 , -0.851559 , -0.850547 , -0.850547 , -0.850287 , -0.850287 , -0.849293 ],
[ 6.99433 , 3.28677e-06 , -5.45556e-05 , -0.851322 , -0.850520 , -0.850520 , -0.850321 , -0.850321 , -0.849531 ],
[ 7.18336 , 1.55256e-06 , -4.61875e-05 , -0.851134 , -0.850499 , -0.850499 , -0.850346 , -0.850346 , -0.849719 ],
[ 7.3724 , 4.57149e-07 , -3.91539e-05 , -0.850986 , -0.850483 , -0.850483 , -0.850366 , -0.850366 , -0.849868 ],
[ 7.56144 , -2.50044e-07 , -3.3231e-05 , -0.850869 , -0.850471 , -0.850471 , -0.850380 , -0.850380 , -0.849986 ],
[ 7.75047 , -7.01547e-07 , -2.82527e-05 , -0.850776 , -0.850461 , -0.850461 , -0.850392 , -0.850392 , -0.850079 ],
[ 7.93951 , -9.693e-07 , -2.40853e-05 , -0.850703 , -0.850454 , -0.850454 , -0.850400 , -0.850400 , -0.850153 ],
[ 8.12854 , -1.09914e-06 , -2.06105e-05 , -0.850645 , -0.850448 , -0.850448 , -0.850407 , -0.850407 , -0.850211 ],
[ 8.31758 , -1.12749e-06 , -1.7719e-05 , -0.850600 , -0.850444 , -0.850444 , -0.850412 , -0.850412 , -0.850257 ],
[ 8.50662 , -1.08659e-06 , -1.53105e-05 , -0.850564 , -0.850440 , -0.850440 , -0.850416 , -0.850416 , -0.850294 ],
[ 8.69565 , -1.00439e-06 , -1.32963e-05 , -0.850536 , -0.850438 , -0.850438 , -0.850419 , -0.850419 , -0.850322 ],
[ 8.88469 , -9.0295e-07 , -1.16011e-05 , -0.850513 , -0.850436 , -0.850436 , -0.850421 , -0.850421 , -0.850345 ],
[ 9.07372 , -7.97673e-07 , -1.01637e-05 , -0.850495 , -0.850434 , -0.850434 , -0.850423 , -0.850423 , -0.850363 ],
[ 9.26276 , -6.977e-07 , -8.93627e-06 , -0.850482 , -0.850433 , -0.850433 , -0.850425 , -0.850425 , -0.850377 ],
[ 9.4518 , -6.07201e-07 , -7.88179e-06 , -0.850470 , -0.850432 , -0.850432 , -0.850426 , -0.850426 , -0.850389 ],
[ 9.64083 , -5.27071e-07 , -6.97177e-06 , -0.850462 , -0.850432 , -0.850432 , -0.850427 , -0.850427 , -0.850398 ],
[ 9.82987 , -4.56455e-07 , -6.18389e-06 , -0.850455 , -0.850431 , -0.850431 , -0.850428 , -0.850428 , -0.850405 ],
[ 10.0189 , -3.93899e-07 , -5.50014e-06 , -0.850450 , -0.850431 , -0.850431 , -0.850428 , -0.850428 , -0.850410 ],
[ 10.2079 , -3.38041e-07 , -4.90551e-06 , -0.850445 , -0.850431 , -0.850431 , -0.850429 , -0.850429 , -0.850415 ],
[ 10.397 , -2.87888e-07 , -4.38722e-06 , -0.850442 , -0.850431 , -0.850431 , -0.850429 , -0.850429 , -0.850418 ],
[ 10.586 , -2.42876e-07 , -3.93422e-06 , -0.850439 , -0.850431 , -0.850431 , -0.850429 , -0.850429 , -0.850421 ],
[ 10.775 , -2.02724e-07 , -3.53699e-06 , -0.850437 , -0.850430 , -0.850430 , -0.850429 , -0.850429 , -0.850423 ]
]

neon_param = fit_model(neon_ref_data, model_parameters)
print("neon rms error =", model_error(neon_ref_data,neon_param))
print(neon_param)

#completed output of Neon parameter fitting saved for posterity:
neon_param = {'coulomb_p': -0.010255409806855187,
              'coulomb_s': 0.4536486561938202,
              'dipole': 1.6692376991516769,
              'energy_p': -3.1186533988406335,
              'energy_s': 11.334912902362603,
              'r_hop': 2.739689713337267,
              'r_pseudo': 1.1800779720963734,
              't_pp1': -0.029546671673199854,
              't_pp2': -0.0041958662271044875,
              't_sp': 0.000450562836426027,
              't_ss': 0.0289251941290921,
              'v_pseudo': -0.015945813280635074}


# In[ ]:




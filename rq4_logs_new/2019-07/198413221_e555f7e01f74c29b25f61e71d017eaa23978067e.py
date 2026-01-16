class MP2(Hartree_Fock):
	def __init__(self, ionic_charge, fock_matrix, chi_tensor, interaction_matrix, atomic_coordinate, energy_scf, energy_ion):
		super().__init__(ionic_charge, fock_matrix, chi_tensor, interaction_matrix, atomic_coordinates, energy_scf, energy_ion)
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
		num_occ = ((self.ionic_charge // 2) * np.size(self.fock_matrix, 0) // self.orbitals_per_atom)
		orbital_energy, orbital_matrix = np.linalg.eigh(self.fock_matrix)
		occupied_energy = orbital_energy[:num_occ]
		virtual_energy = orbital_energy[num_occ:]
		occupied_matrix = orbital_matrix[:, :num_occ]
		virtual_matrix = orbital_matrix[:, num_occ:]

		return occupied_energy, virtual_energy, occupied_matrix, virtual_matrix

	def transform_interaction_tensor(self):
		"""Returns a transformed V tensor defined by the input occupied, virtual, & interaction matrices.
        Parameters
        ----------
        occupied_matrix : numpy.ndarray
            A rank 2 array, [:, :num_occ], indexed by the number of basis functions/orbitals
            and the number of occupied orbitals. The stored values are 'floats'.
        virtual_matrix : numpy.ndarray
            A rank 2 array, [:, num_occ:], indexed by the number of virtual orbitals and by the
            number of basis functions/orbitals. The stored values are 'floats'.
        interaction_matrix : numpy.ndarray
            A (ndof,ndof) array populated with 'float' data types. The elements of this matrix are
            constructed by numerically computing an expectation value for the pairwise coulomic interactions
            between electrons in orbitals {p} and {q}.
        chi_tensor : numpy.ndarray
            A rank 3 array, [ndof,ndof,ndof], indexed by {p} and {q} atomic orbitals and
            r--the multipole moment index. The stored values are 'floats'.
        Returns
        -------
        interaction_tensor : numpy.ndarray
            A rank 4 interaction tensor represented by an einstein sum over {p} and {q} is contracted to
            4 indices: aibj. These particle-hole or O-V indices define the basis featured in the second
            order Moller-Plesset perturbation energy expression.
        """
		chi2_tensor = np.einsum('qa,ri,qrp', self.virtual_matrix, self.occupied_matrix, self.chi_tensor, optimize=True)
		interaction_tensor = np.einsum('aip,pq,bjq->aibj', chi2_tensor, self.interaction_matrix, chi2_tensor, optimize=True)
		return interaction_tensor

	def calculate_energy_mp2(self):
		"""Returns the MP2 contribution to the total energy defined by the input Fock & interaction matrices.
        Parameters
        ----------
        fock_matrix : numpy.ndarray
            A (ndof,ndof) array populated with 'float' data types. The elements of this matrix are constructed by
            numerically computing an expectation value for the fock operator operating on a state vector.
            The transformed state is then projected onto an orbital basis.
        interaction_matrix : numpy.ndarray
            A (ndof,ndof) array populated with 'float' data types. The elements of this matrix are
            constructed by numerically computing an expectation value for the pairwise coulomic interactions
            between electrons in orbitals {p} and {q}.
        chi_tensor : numpy.ndarray
            A rank 3 array, [ndof,ndof,ndof], indexed by {p} and {q} atomic orbitals and
            r--the multipole moment index. The stored values are 'floats'.
        Returns
        -------
        energy_mp2 : numpy.float64
            The MP2 energy is a scalar that represents the sum of the HF energy and the energy correction
            computed using second order Moller-Plesset perturbation thoery.
        """
		num_occ = ((self.ionic_charge // 2) * np.size(self.fock_matrix, 0) // self.orbitals_per_atom)
		num_virt = (len(self.atomic_coordinates) * self.orbitals_per_atom) - num_occ

		energy_mp2 = 0.0
        num_occ = len(self.occupied_energy)
        num_virt = len(self.virtual_energy)
        for a in range(num_virt):
            for b in range(num_virt):
                for i in range(num_occ):
                    for j in range(num_occ):
                        energy_mp2 -= ((2.0 * self.interaction_tensor[a, i, b, j]**2 - self.interaction_tensor[a, i, b, j] * 
										self.interaction_tensor[a, j, b, i]) / (self.vitual_energy[a] + self.vitual_energy[b] - 
										self.occupied_enery[i] - self.occupied_energy[j]))
        return energy_mp2

	def calculate_total_energy(self):
		"""Returns the total energy of the calculation, Hartree-Fock + Ion + Second order energy correction
		Parameters
		----------
		energy_scf: float
		energy_ion: float
		energy_mp2: float

		Returns
		-------
		total_energy: float

		"""
		total_energy = self.energy_scf + self.energy.ion + self.energy_mp2
		return total_energy
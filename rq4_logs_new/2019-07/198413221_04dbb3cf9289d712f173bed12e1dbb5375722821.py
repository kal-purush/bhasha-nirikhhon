
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
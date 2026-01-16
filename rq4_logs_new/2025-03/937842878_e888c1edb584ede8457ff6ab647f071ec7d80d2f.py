import numpy as np

def local_elastic_stiffness_matrix_3D_beam(E: float, nu: float, A: float, L: float, Iy: float, Iz: float, J: float) -> np.ndarray:
    """
    local element elastic stiffness matrix
    source: p. 73 of McGuire's Matrix Structural Analysis 2nd Edition
    Given:
        material and geometric parameters:
            A, L, Iy, Iz, J, nu, E
    Context:
        load vector:
            [Fx1, Fy1, Fz1, Mx1, My1, Mz1, Fx2, Fy2, Fz2, Mx2, My2, Mz2]
        DOF vector:
            [u1, v1, w1, th_x1, th_y1, th_z1, u2, v2, w2, th_x2, th_y2, th_z2]
        Equation:
            [load vector] = [stiffness matrix] @ [DOF vector]
    Returns:
        12 x 12 elastic stiffness matrix k_e
    """
    k_e = np.zeros((12, 12))
    # Axial terms - extension of local x axis
    axial_stiffness = E * A / L
    k_e[0, 0] = axial_stiffness
    k_e[0, 6] = -axial_stiffness
    k_e[6, 0] = -axial_stiffness
    k_e[6, 6] = axial_stiffness
    # Torsion terms - rotation about local x axis
    torsional_stiffness = E * J / (2.0 * (1 + nu) * L)
    k_e[3, 3] = torsional_stiffness
    k_e[3, 9] = -torsional_stiffness
    k_e[9, 3] = -torsional_stiffness
    k_e[9, 9] = torsional_stiffness
    # Bending terms - bending about local z axis
    k_e[1, 1] = E * 12.0 * Iz / L ** 3.0
    k_e[1, 7] = E * -12.0 * Iz / L ** 3.0
    k_e[7, 1] = E * -12.0 * Iz / L ** 3.0
    k_e[7, 7] = E * 12.0 * Iz / L ** 3.0
    k_e[1, 5] = E * 6.0 * Iz / L ** 2.0
    k_e[5, 1] = E * 6.0 * Iz / L ** 2.0
    k_e[1, 11] = E * 6.0 * Iz / L ** 2.0
    k_e[11, 1] = E * 6.0 * Iz / L ** 2.0
    k_e[5, 7] = E * -6.0 * Iz / L ** 2.0
    k_e[7, 5] = E * -6.0 * Iz / L ** 2.0
    k_e[7, 11] = E * -6.0 * Iz / L ** 2.0
    k_e[11, 7] = E * -6.0 * Iz / L ** 2.0
    k_e[5, 5] = E * 4.0 * Iz / L
    k_e[11, 11] = E * 4.0 * Iz / L
    k_e[5, 11] = E * 2.0 * Iz / L
    k_e[11, 5] = E * 2.0 * Iz / L
    # Bending terms - bending about local y axis
    k_e[2, 2] = E * 12.0 * Iy / L ** 3.0
    k_e[2, 8] = E * -12.0 * Iy / L ** 3.0
    k_e[8, 2] = E * -12.0 * Iy / L ** 3.0
    k_e[8, 8] = E * 12.0 * Iy / L ** 3.0
    k_e[2, 4] = E * -6.0 * Iy / L ** 2.0
    k_e[4, 2] = E * -6.0 * Iy / L ** 2.0
    k_e[2, 10] = E * -6.0 * Iy / L ** 2.0
    k_e[10, 2] = E * -6.0 * Iy / L ** 2.0
    k_e[4, 8] = E * 6.0 * Iy / L ** 2.0
    k_e[8, 4] = E * 6.0 * Iy / L ** 2.0
    k_e[8, 10] = E * 6.0 * Iy / L ** 2.0
    k_e[10, 8] = E * 6.0 * Iy / L ** 2.0
    k_e[4, 4] = E * 4.0 * Iy / L
    k_e[10, 10] = E * 4.0 * Iy / L
    k_e[4, 10] = E * 2.0 * Iy / L
    k_e[10, 4] = E * 2.0 * Iy / L
    return k_e


def check_unit_vector(vec: np.ndarray):
    """
    """
    if np.isclose(np.linalg.norm(vec), 1.0):
        return
    else:
        raise ValueError("Expected a unit vector for reference vector.")


def check_parallel(vec_1: np.ndarray, vec_2: np.ndarray):
    """
    """
    if np.isclose(np.linalg.norm(np.cross(vec_1, vec_2)), 0.0):
        raise ValueError("Reference vector is parallel to beam axis.")
    else:
        return


def rotation_matrix_3D(x1: float, y1: float, z1: float, x2: float, y2: float, z2: float, v_temp: np.ndarray = None):
    """
    3D rotation matrix
    source: Chapter 5.1 of McGuire's Matrix Structural Analysis 2nd Edition
    Given:
        x, y, z coordinates of the ends of two beams: x1, y1, z1, x2, y2, z2
        optional: reference z vector direction v_temp to orthonormalize the local y and z axis
            if v_temp is not given, VVVV
    Compute:
        where l, m, n are defined as direction cosines:
        gamma = [[lx'=cos alpha_x', mx'=cos beta_x', nx'=cos gamma_x'],
                 [ly'=cos alpha_y', my'=cos beta_y', ny'=cos gamma_y'],
                 [lz'=cos alpha_z', mz'=cos beta_z', nz'=cos gamma_z']]
    """
    L = np.sqrt((x2 - x1) ** 2.0 + (y2 - y1) ** 2.0 + (z2 - z1) ** 2.0)
    lxp = (x2 - x1) / L
    mxp = (y2 - y1) / L
    nxp = (z2 - z1) / L
    local_x = np.asarray([lxp, mxp, nxp])

    # choose a vector to orthonormalize the y axis if one is not given
    if v_temp is None:
        # if the beam is oriented vertically, switch to the global y axis
        if np.isclose(lxp, 0.0) and np.isclose(mxp, 0.0):
            v_temp = np.array([0, 1.0, 0.0])
        else:
            # otherwise use the global z axis
            v_temp = np.array([0, 0, 1.0])
    else:
        # check to make sure that given v_temp is a unit vector
        check_unit_vector(v_temp)
        # check to make sure that given v_temp is not parallel to the local x axis
        check_parallel(local_x, v_temp)
    
    # compute the local y axis
    local_y = np.cross(v_temp, local_x)
    local_y = local_y / np.linalg.norm(local_y)

    # compute the local z axis
    local_z = np.cross(local_x, local_y)
    local_z = local_z / np.linalg.norm(local_z)

    # assemble R
    gamma = np.vstack((local_x, local_y, local_z))
    
    return gamma


def transformation_matrix_3D(gamma: np.ndarray) -> np.ndarray:
    """
    3D transformation matrix
    source: Chapter 5.1 of McGuire's Matrix Structural Analysis 2nd Edition
    Given:
        gamma -- the 3x3 rotation matrix
    Compute:
        Gamma -- the 12x12 transformation matrix
    """
    Gamma = np.zeros((12, 12))
    Gamma[0:3, 0:3] = gamma
    Gamma[3:6, 3:6] = gamma
    Gamma[6:9, 6:9] = gamma
    Gamma[9:12, 9:12] = gamma
    return Gamma


def local_geometric_stiffness_matrix_3D_beam(L, A, I_rho, Fx2, Mx2, My1, Mz1, My2, Mz2):
    """
    local element geometric stiffness matrix
    source: p. 258 of McGuire's Matrix Structural Analysis 2nd Edition
    Given:
        material and geometric parameters:
            L, A, I_rho (polar moment of inertia)
        element forces and moments:
            Fx2, Mx2, My1, Mz1, My2, Mz2
    Context:
        load vector:
            [Fx1, Fy1, Fz1, Mx1, My1, Mz1, Fx2, Fy2, Fz2, Mx2, My2, Mz2]
        DOF vector:
            [u1, v1, w1, th_x1, th_y1, th_z1, u2, v2, w2, th_x2, th_y2, th_z2]
        Equation:
            [load vector] = [stiffness matrix] @ [DOF vector]
    Returns:
        12 x 12 geometric stiffness matrix k_g
    """
    k_g = np.zeros((12, 12))
    # upper triangle off diagonal terms
    k_g[0, 6] = -Fx2 / L
    k_g[1, 3] = My1 / L
    k_g[1, 4] = Mx2 / L
    k_g[1, 5] = Fx2 / 10.0
    k_g[1, 7] = -6.0 * Fx2 / (5.0 * L)
    k_g[1, 9] = My2 / L
    k_g[1, 10] = -Mx2 / L
    k_g[1, 11] = Fx2 / 10.0
    k_g[2, 3] = Mz1 / L
    k_g[2, 4] = -Fx2 / 10.0
    k_g[2, 5] = Mx2 / L
    k_g[2, 8] = -6.0 * Fx2 / (5.0 * L)
    k_g[2, 9] = Mz2 / L
    k_g[2, 10] = -Fx2 / 10.0
    k_g[2, 11] = -Mx2 / L
    k_g[3, 4] = -1.0 * (2.0 * Mz1 - Mz2) / 6.0
    k_g[3, 5] = (2.0 * My1 - My2) / 6.0
    k_g[3, 7] = -My1 / L
    k_g[3, 8] = -Mz1 / L
    k_g[3, 9] = -Fx2 * I_rho / (A * L)
    k_g[3, 10] = -1.0 * (Mz1 + Mz2) / 6.0
    k_g[3, 11] = (My1 + My2) / 6.0
    k_g[4, 7] = -Mx2 / L
    k_g[4, 8] = Fx2 / 10.0
    k_g[4, 9] = -1.0 * (Mz1 + Mz2) / 6.0
    k_g[4, 10] = -Fx2 * L / 30.0
    k_g[4, 11] = Mx2 / 2.0
    k_g[5, 7] = -Fx2 / 10.0
    k_g[5, 8] = -Mx2 / L
    k_g[5, 9] = (My1 + My2) / 6.0
    k_g[5, 10] = -Mx2 / 2.0
    k_g[5, 11] = -Fx2 * L / 30.0
    k_g[7, 9] = -My2 / L
    k_g[7, 10] = Mx2 / L
    k_g[7, 11] = -Fx2 / 10.0
    k_g[8, 9] = -Mz2 / L
    k_g[8, 10] = Fx2 / 10.0
    k_g[8, 11] = Mx2 / L
    k_g[9, 10] = (Mz1 - 2.0 * Mz2) / 6.0
    k_g[9, 11] = -1.0 * (My1 - 2.0 * My2) / 6.0
    # add in the symmetric lower triangle
    k_g = k_g + k_g.transpose()
    # add diagonal terms
    k_g[0, 0] = Fx2 / L
    k_g[1, 1] = 6.0 * Fx2 / (5.0 * L)
    k_g[2, 2] = 6.0 * Fx2 / (5.0 * L)
    k_g[3, 3] = Fx2 * I_rho / (A * L)
    k_g[4, 4] = 2.0 * Fx2 * L / 15.0
    k_g[5, 5] = 2.0 * Fx2 * L / 15.0
    k_g[6, 6] = Fx2 / L
    k_g[7, 7] = 6.0 * Fx2 / (5.0 * L)
    k_g[8, 8] = 6.0 * Fx2 / (5.0 * L)
    k_g[9, 9] = Fx2 * I_rho / (A * L)
    k_g[10, 10] = 2.0 * Fx2 * L / 15.0
    k_g[11, 11] = 2.0 * Fx2 * L / 15.0
    return k_g


def local_geometric_stiffness_matrix_3D_beam_without_interaction_terms(L, A, I_rho, Fx2):
    """
    local element geometric stiffness matrix
    source: p. 257 of McGuire's Matrix Structural Analysis 2nd Edition
    Given:
        material and geometric parameters:
            L, A, I_rho (polar moment of inertia)
        element forces and moments:
            Fx2
    Context:
        load vector:
            [Fx1, Fy1, Fz1, Mx1, My1, Mz1, Fx2, Fy2, Fz2, Mx2, My2, Mz2]
        DOF vector:
            [u1, v1, w1, th_x1, th_y1, th_z1, u2, v2, w2, th_x2, th_y2, th_z2]
        Equation:
            [load vector] = [stiffness matrix] @ [DOF vector]
    Returns:
        12 x 12 geometric stiffness matrix k_g
    """
    k_g = np.zeros((12, 12))
    # upper triangle off diagonal terms
    k_g[0, 6] = -Fx2 / L
    k_g[1, 5] = Fx2 / 10.0
    k_g[1, 7] = -6.0 * Fx2 / (5.0 * L)
    k_g[1, 11] = Fx2 / 10.0
    k_g[2, 4] = -Fx2 / 10.0
    k_g[2, 8] = -6.0 * Fx2 / (5.0 * L)
    k_g[2, 10] = -Fx2 / 10.0
    k_g[3, 9] = -Fx2 * I_rho / (A * L)
    k_g[4, 8] = Fx2 / 10.0
    k_g[4, 10] = -Fx2 * L / 30.0
    k_g[5, 7] = -Fx2 / 10
    k_g[5, 11] = -Fx2 * L / 30.0
    k_g[7, 11] = -Fx2 / 10.0
    k_g[8, 10] = Fx2 / 10.0
    # add in the symmetric lower triangle
    k_g = k_g + k_g.transpose()
    # add diagonal terms
    k_g[0, 0] = Fx2 / L
    k_g[1, 1] = 6.0 * Fx2 / (5.0 * L)
    k_g[2, 2] = 6.0 * Fx2 / (5.0 * L)
    k_g[3, 3] = Fx2 * I_rho / (A * L)
    k_g[4, 4] = 2.0 * Fx2 * L / 15.0
    k_g[5, 5] = 2.0 * Fx2 * L / 15.0
    k_g[6, 6] = Fx2 / L
    k_g[7, 7] = 6.0 * Fx2 / (5.0 * L)
    k_g[8, 8] = 6.0 * Fx2 / (5.0 * L)
    k_g[9, 9] = Fx2 * I_rho / (A * L)
    k_g[10, 10] = 2.0 * Fx2 * L / 15.0
    k_g[11, 11] = 2.0 * Fx2 * L / 15.0
    return k_g

def K_assembly(num_nodes, dof_per_node, k_element):
    
    K = np.zeros([num_nodes * dof_per_node, num_nodes * dof_per_node])

    connectivity = np.zeros([num_nodes - 1, 2])

    for i in range(1, num_nodes):
        if i == 1:
            connectivity[(i - 1), 0] = i - 1
            connectivity[(i - 1), 1] = connectivity[(i - 1), 0] + 1
        else:
            connectivity[(i - 1), 0] = connectivity[(i - 2), 1]
            connectivity[(i - 1), 1] = connectivity[(i - 1), 0] + 1

    # Assembly process
    for elem in range(connectivity.shape[0]):  # Loop over elements
        nodes = connectivity[elem]  # Get nodes for this element
    
    # Compute global DOF indices for this element
        global_dof_indices = np.concatenate([
            np.arange(6 * nodes[0], 6 * nodes[0] + 6),  # DOFs of first node
            np.arange(6 * nodes[1], 6 * nodes[1] + 6)   # DOFs of second node
        ])
    
    # Assemble local stiffness matrix into the global stiffness matrix
        for p in range(12):  # Local row
            for q in range(12):  # Local column
                global_p = int(global_dof_indices[p])  # Map local to global row
                global_q = int(global_dof_indices[q]) # Map local to global column
            
                K[global_p, global_q] += k_element[p, q]  # Assemble
    return K
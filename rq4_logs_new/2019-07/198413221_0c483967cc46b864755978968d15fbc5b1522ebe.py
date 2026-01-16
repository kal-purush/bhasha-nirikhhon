import qm_project_python_testing as qm
import pytest
import numpy as np

#refer to the original jupyter notebook for outputs

#define system w/o file call
system = qm.Nobel_Gas_model('argon')
#define coordinates w/o file call
atomic_coordinates = np.array([ [0.0,0.0,0.0], [3.0,4.0,5.0] ])

#testing hopping_energy

def test_hopping_energy():

    test_r12 = np.array([-system.model_parameters['r_hop'],0,0])
    calculated_ans = qm.hopping_energy('s', 's', test_r12, system.model_parameters, system)
    expected_ans =  0.03365982238611262
    #assert expected_ans == calculated_ans
    assert np.isclose(expected_ans, calculated_ans)

#testing coulomb energy

def test_coulomb_energy():
    test_r12 = np.array([-1,0,0])
    expected_ans = 1.0
    calculated_ans = qm.coulomb_energy('s', 's', test_r12, system)
    assert np.isclose(expected_ans, calculated_ans)

#testing pseudopotential_energy

def test_pseudopotential_energy(): # what/where are o and r?
    r = np.array([-system.model_parameters['r_pseudo'],0,0])
    expected_ans = 0.022972992186364977
    calculated_ans = qm.pseudopotential_energy('s', r, system.model_parameters, system)
    assert np.isclose(expected_ans, calculated_ans)

#testing chi_on_atom

def test_chi_on_atom(): #how to verify a return value?
    test_return = qm.chi_on_atom('s', 's', 's', system.model_parameters, system)
    assert test_return == 1

#testing calculate_energy_ion
def test_calculate_energy_ion():
    expected_ans = 5.091168824543142
    calculated_ans = qm.calculate_energy_ion(atomic_coordinates,system)
    assert np.isclose(expected_ans, calculated_ans)

#testing calculate_potential_vector
def test_calculate_potential_vector():
    expected_ans = (np.array([-0.84848908, -0.05082167, -0.06776222, -0.08470278,
    -0.84848908, 0.05082167, 0.06776222 ,0.08470278]))
    calculated_ans = qm.calculate_potential_vector(atomic_coordinates, system.model_parameters, system)
    assert np.allclose(expected_ans, calculated_ans) == True

#testing calculate_interaction_matrix
def test_calculate_interaction_matrix():
    expected_ans = (np.array([[ 3.60353329e-01,  0.00000000e+00,  0.00000000e+00,  0.00000000e+00,
   1.41421356e-01, -8.48528137e-03, -1.13137085e-02, -1.41421356e-02],
 [ 0.00000000e+00, -3.26799184e-03,  0.00000000e+00,  0.00000000e+00,
   8.48528137e-03,  1.30107648e-03, -2.03646753e-03, -2.54558441e-03],
 [ 0.00000000e+00,  0.00000000e+00, -3.26799184e-03,  0.00000000e+00,
   1.13137085e-02, -2.03646753e-03,  1.13137085e-04, -3.39411255e-03],
 [ 0.00000000e+00,  0.00000000e+00,  0.00000000e+00, -3.26799184e-03,
   1.41421356e-02, -2.54558441e-03, -3.39411255e-03, -1.41421356e-03],
 [ 1.41421356e-01,  8.48528137e-03,  1.13137085e-02,  1.41421356e-02,
   3.60353329e-01,  0.00000000e+00,  0.00000000e+00,  0.00000000e+00],
 [-8.48528137e-03,  1.30107648e-03, -2.03646753e-03, -2.54558441e-03,
   0.00000000e+00, -3.26799184e-03,  0.00000000e+00,  0.00000000e+00],
 [-1.13137085e-02, -2.03646753e-03,  1.13137085e-04, -3.39411255e-03,
   0.00000000e+00,  0.00000000e+00, -3.26799184e-03,  0.00000000e+00,],
 [-1.41421356e-02, -2.54558441e-03, -3.39411255e-03, -1.41421356e-03,
   0.00000000e+00,  0.00000000e+00,  0.00000000e+00, -3.26799184e-03]]))

    calculated_ans = qm.calculate_interaction_matrix(atomic_coordinates, system.model_parameters,system)
    assert np.allclose(expected_ans, calculated_ans) == True

#testing calculate_chi_tensor

def testing_calculate_chi_tensor():
    expected_ans = (np.array([[[1.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         2.78162928, 0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         2.78162928, 0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         2.78162928, 0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0. ,        0.,        0.,
   0.,         0.        ]], #might need , here

 [[0.,         2.78162928, 0.,         0.,         0.,         0.,
   0.,         0.        ],
  [1.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,        0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,        0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,        0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ]], #might need , here

 [[0.,         0.,         2.78162928, 0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,        0.,
   0.,         0.        ],
  [1.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ]], #might need , here

 [[0.,         0.,         0.,         2.78162928, 0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,        0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [1.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ]], #might need  , here

 [[0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         1.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,        0.,         0.,         2.78162928,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   2.78162928, 0.        ],
  [0.,         0.,        0.,         0.,        0.,         0.,
   0.,         2.78162928]], #might need , here

 [[0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         2.78162928,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         1.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ]], #might need , here

 [[0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,        0.,        0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   2.78162928, 0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         1.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ]], #might need , here

 [[0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,        0.,
   0.,         2.78162928],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         0.,         0.,
   0.,         0.        ],
  [0.,         0.,         0.,         0.,         1.,         0.,
   0.,         0.        ]]]))

    calculated_ans = qm.calculate_chi_tensor(atomic_coordinates, system.model_parameters, system)
    assert np.allclose(expected_ans, calculated_ans) == True




#testing calculate_hamiltonian_matrix
def test_hamiltonian_matrix():
    expected_ans =  (np.array([[ 2.31745554e+00, -1.41367040e-01, -1.88489387e-01, -2.35611734e-01,
   6.53808346e-04,  5.34076767e-04,  7.12102356e-04,  8.90127945e-04],
 [-1.41367040e-01, -3.24117641e+00,  0.00000000e+00,  0.00000000e+00,
  -5.34076767e-04,  2.92479412e-04,  2.17340049e-03,  2.71675062e-03],
 [-1.88489387e-01,  0.00000000e+00, -3.24117641e+00,  0.00000000e+00,
  -7.12102356e-04,  2.17340049e-03,  1.56029637e-03,  3.62233416e-03],
 [-2.35611734e-01,  0.00000000e+00,  0.00000000e+00, -3.24117641e+00,
  -8.90127945e-04,  2.71675062e-03,  3.62233416e-03,  3.19034674e-03],
 [ 6.53808346e-04, -5.34076767e-04, -7.12102356e-04, -8.90127945e-04,
   2.31745554e+00,  1.41367040e-01,  1.88489387e-01,  2.35611734e-01],
 [ 5.34076767e-04,  2.92479412e-04,  2.17340049e-03,  2.71675062e-03,
   1.41367040e-01, -3.24117641e+00,  0.00000000e+00,  0.00000000e+00],
 [ 7.12102356e-04,  2.17340049e-03,  1.56029637e-03,  3.62233416e-03,
   1.88489387e-01,  0.00000000e+00, -3.24117641e+00,  0.00000000e+00],
 [ 8.90127945e-04,  2.71675062e-03,  3.62233416e-03,  3.19034674e-03,
   2.35611734e-01,  0.00000000e+00,  0.00000000e+00, -3.24117641e+00]]))

    calculated_ans = qm.calculate_hamiltonian_matrix(atomic_coordinates, system.model_parameters, system)
    assert np.allclose(expected_ans, calculated_ans) == True

#testing calculate_atomic_density_matrix
def test_calculate_atomic_density_matrix():
    expected_ans = (np.array([[0., 0., 0., 0., 0., 0., 0., 0.],
 [0., 1., 0., 0., 0., 0., 0., 0.],
 [0., 0., 1., 0., 0., 0., 0., 0.],
 [0., 0., 0., 1., 0., 0., 0., 0.],
 [0., 0., 0., 0., 0., 0., 0., 0.],
 [0., 0., 0., 0., 0., 1., 0., 0.],
 [0., 0., 0., 0., 0., 0., 1., 0.],
 [0., 0., 0., 0., 0., 0., 0., 1.]]))

    calculated_ans = qm.calculate_atomic_density_matrix(atomic_coordinates, system)
    assert np.allclose(expected_ans, calculated_ans) == True
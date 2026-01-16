import src.utils as utils
import numpy as np
import src.partition as pou
import pickle
import sys

"""
This code overwrites constant cH in res.pickle results
as well as impacted estimator
"""

# User input
basis = str(sys.argv[1]) # GTO basis set

femfile = "dat/helfem.chk"
resfile = "out/res.pickle"   
denfile = "dat/density.hdf5"

with open(resfile, 'rb') as file:
    data = pickle.load(file)
    estim_atom = data[basis]["estim_atom"]
    estim_Delta = data[basis]["estim_Delta"]
    c1 = data[basis]["c1"]
    c2 = data[basis]["c2"]
    shift = data[basis]["shift"]
    shift1 = data[basis]["shift1"]
    shift2 = data[basis]["shift2"]
    shift3 = data[basis]["shift3"]
    amin = data[basis]["amin"]
    amax = data[basis]["amax"]


# Read data Diatomic
dV, Rh, helfem_grid, wquad, u_fem, Z1, Z2 = utils.diatomic_density(denfile)

# Reference FEM solution from HelFEM
Efem, E2, Efem_kin, Efem_nuc, Efem_nucr = utils.diatomic_energy(femfile)
Efem = Efem - Efem_nucr + shift

# Constant of Assumption 3
cH = 1./np.sqrt(Efem)
# Constant associated to partition (equation 1.3)
delta = pou.delta_value(amin, amax)
sigmas = (shift1, shift2, shift3, shift)
val_sup = pou.eval_supremum(amin, amax, Rh, Z1, Z2, sigmas, delta)
cP = 1 + cH**2 * val_sup

r1 = 2*estim_atom + estim_Delta
final_estim = pow(cP * 1./c1 * r1 + Efem * cP**2 * 1./c2**2 * r1**2, 0.5)

print("cH=", cH)
print("cP=", cP)
print("Estimator of Theorem 3.7=", final_estim)

# Store results to file
data = {}
data["cP"] = cP
data["cH"] = cH
data["estimator"] = final_estim
key = basis
utils.store_to_file(resfile, key, data)


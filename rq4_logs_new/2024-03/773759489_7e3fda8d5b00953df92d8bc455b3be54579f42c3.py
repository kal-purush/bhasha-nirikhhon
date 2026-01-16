import copy

import afterglowpy as grb
import h5py, os, numpy as np, time
import pandas as pd
from multiprocessing import Pool
import matplotlib.pyplot as plt
from matplotlib import cm, ticker
from itertools import product
from tqdm import tqdm

pc = 3.0857e18 # cm # parserc

class RunAfterglowPy():
    Z = {'jetType':     grb.jet.TopHat,     # Top-Hat jet
         'specType':    0,                  # Basic Synchrotron Spectrum
         # 'counterjet':  1,
         # 'spread':      0,
         'thetaObs':    0.0,   # Viewing angle in radians
         'E0':          1.0e52, # Isotropic-equivalent energy in erg
         'g0':          1000,
         'thetaCore':   0.2,    # Half-opening angle in radians
         'thetaWing':   0.2,
         'n0':          1e-3,    # circumburst density in cm^{-3}
         'p':           2.2,    # electron energy distribution index
         'epsilon_e':   0.1,    # epsilon_e
         'epsilon_B':   0.01,   # epsilon_B
         'xi_N':        1.0,    # Fraction of electrons accelerated
         'd_L':         3.09e26, # Luminosity distance in cm
         'z':           0.0099}   # redshift
    mapping = {"theta_obs":"thetaObs",
               "Eiso_c":"E0",
            #    "Gamma0c":"g0",
               "n_ism":"n0",
               "theta_c":"thetaCore",
               "theta_w":"thetaWing",
               "p_fs":"p",
               "eps_e_fs":"epsilon_e",
               "eps_b_fs":"epsilon_B",
               "d_l":"d_L",
               "z":"z"
               }
    times = np.logspace(2, 3, 128) * (3600 * 24)
    freqs = np.logspace(8, 27, 64)
    times_, freqs_ = np.meshgrid(times, freqs, indexing='ij')
    def __init__(self, par_list: list):
        self.par_list = par_list

    def __call__(self, idx):
        pars = copy.deepcopy(self.par_list[idx])
        Z = copy.deepcopy(self.Z)
        for key, val in pars.items():
            if key in self.mapping.keys():
                Z[self.mapping[key]] = val
        Fnu = grb.fluxDensity(self.times_.flatten(), self.freqs_.flatten(), **Z)
        Fnu = np.reshape(Fnu, newshape=self.times_.shape)
        vals = np.array([pars[key] for key in pars.keys()])
        return (vals, Fnu)

if __name__ == '__main__':
    
    pars = {
        "n_ism": np.asarray([1.0, 0.1, 0.01, 0.001]),
        # "theta_obs": np.array([0., 15., 45.0, 60., 75., 90.]) * np.pi / 180.0,
        "theta_obs": np.array([0., 45.0, 75., 90.]) * np.pi / 180.0,
        "Eiso_c": np.asarray([1.e50, 1.e51, 1.e52, 1.e53]),
        # "Gamma0c":[100., 300., 600., 1000.],
        "theta_c": np.array([5., 10., 15., 20.]) * np.pi / 180.,
        "p_fs": [2.2, 2.4, 2.6, 2.8],
        "eps_e_fs": [0.5, 0.1, 0.01, 0.001],
        "eps_b_fs": [0.5, 0.1, 0.01, 0.001],
    }

    ranges = [pars[key] for key in pars.keys()]
    result = []
    all_combinations = product(*ranges)
    
    # Get the number of combinations in a hacky way: 
    len_list = [len(pars[key]) for key in pars.keys()]
    nb_combinations = np.prod(len_list)
    
    for combination in all_combinations:
        new_entry = {par:val for par,val in zip(pars.keys(),combination)}
        new_entry["theta_w"] = new_entry["theta_c"]
        # new_entry["theta_obs"] = 0.0
        result.append(new_entry)

    afgpy = RunAfterglowPy(result)
    start_time = time.perf_counter()

    results = []
    processes = os.cpu_count()
    pool = Pool(processes=20)
    jobs = [pool.apply_async(func=afgpy, args=(*argument,)) if isinstance(argument, tuple)
            else pool.apply_async(func=afgpy, args=(argument,)) for argument in range(len(result))]
    pool.close()
    for job in tqdm(jobs):
        results.append(job.get())

    finish_time = time.perf_counter()

    # format the result to [i_feature_set, i_time, i_freq]
    params = np.vstack([res[0] for res in results])
    spectra = np.stack([res[1] for res in results])

    print(f"Program finished in {finish_time-start_time:.2f} seconds. "
          f"Parms={params.shape} spectra={spectra.shape}")

    # save the final result
    outdir = os.getcwd() + '/'
    with h5py.File(outdir+"X_afgpy.h5","w") as f:
        f.create_dataset("X", data=spectra, dtype=np.float32)
        f.create_dataset("times", data=RunAfterglowPy.times, dtype=np.float64)
        f.create_dataset("freqs", data=RunAfterglowPy.freqs, dtype=np.float64)
    with h5py.File(outdir+"Y_afgpy.h5","w") as f:
        f.create_dataset("Y", data=params)
        f.create_dataset("keys", data=np.array(pars.keys(),dtype="S"))
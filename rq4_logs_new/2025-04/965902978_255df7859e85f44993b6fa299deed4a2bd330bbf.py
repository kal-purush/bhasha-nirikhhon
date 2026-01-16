"""Process BCM data."""

import argparse
import math
import os
import pathlib

import matplotlib.pyplot as plt
import numpy as np
import scipy.interpolate
import xarray as xr
from omegaconf import OmegaConf
from omegaconf import DictConfig
from scipy.constants import speed_of_light

from tools.bcm import load_bcm_waveform
from tools.bcm import interpolate_bcm_waveform
from tools.plotting import set_mpl_style
from tools.utils import coords_to_edges
from tools.utils import edges_to_coords
from tools.utils import get_relativistic_factors


set_mpl_style()


# Setup
# --------------------------------------------------------------------------------------

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--experiment", type=str)
parser.add_argument("--filename", type=str)
args = parser.parse_args()

# Create output directory
path = pathlib.Path(__file__)
experiment = args.experiment
output_dir = os.path.join("outputs", path.stem, experiment)
os.makedirs(output_dir, exist_ok=True)

# Load experiment parameters
cfg = OmegaConf.load("config.yaml")
print(OmegaConf.to_yaml(cfg))

# Calculate beam revolution frequency
gamma, beta = get_relativistic_factors(cfg.beam.kin_energy, cfg.beam.mass)
turn_period = cfg.ring.length / (beta * speed_of_light)
turn_freq = 1.0 / turn_period

print("beta = {}".format(beta))
print("gamma = {}".format(gamma))
print("turn period = {} [s]".format(turn_period))
print("turn frequency = {} [Hz]".format(turn_freq))

# Calculate BCM sampling frequency.
bcm_samp_freq = cfg.bcm.sample_freq
bcm_samp_step = 1.0 / bcm_samp_freq
bcm_samp_per_turn = turn_period / bcm_samp_step

print("BCM sample spacing = {} [s]".format(bcm_samp_step))
print("BCM samples per turn = {} [s]".format(bcm_samp_per_turn))


# Load data
# --------------------------------------------------------------------------------------

input_dir = "../data"
input_dir = os.path.join(input_dir, args.experiment)
filename = os.path.join(input_dir, args.filename)
waveform = load_bcm_waveform(filename, bcm_samp_step)


fig, ax = plt.subplots(figsize=(8.0, 2.0))
ax.plot(waveform[:, 0], waveform[:, 1], color="black")
ax.set_xlabel("Time [s]")
ax.set_ylabel("BCM")
filename = os.path.join(output_dir, "fig_bcm_full")
plt.savefig(filename)
plt.close("all")


# Interpolate data
# --------------------------------------------------------------------------------------

# Since the sampling frequency is an integer multiple of the beam revolution frequency,
# interpolate the waveform onto a new time grid.

samp_per_turn = int(bcm_samp_per_turn + 1)
t_start = cfg.bcm.delay + 0.13e-06
t_step = turn_period / samp_per_turn
waveform = interpolate_bcm_waveform(waveform, t_start, t_step)


# Slice data
# --------------------------------------------------------------------------------------

# Plot slice locations
def plot_bcm_slice(tmin: float, nturns: int = 20):
    fig, ax = plt.subplots(figsize=(8.0, 1.0))
    ax.plot(waveform[:, 0] * 1.00e06, waveform[:, 1], color="black")
    ax.plot(
        waveform[::samp_per_turn, 0] * 1.00e06,
        waveform[::samp_per_turn, 1],
        color="red",
        lw=0,
        marker=".",
    )
    tmax = tmin + nturns * turn_period
    tmin = tmin * 1.00e06
    tmax = tmax * 1.00e06
    ax.set_xlim(tmin, tmax)
    ax.set_xlabel(r"Time [$\mu$s]")
    ax.set_ylabel("BCM")
    return fig, ax


turns = list(range(0, 1000, 100))
for turn in turns:
    tmin = turn_period * turn

    fig, ax = plot_bcm_slice(tmin, nturns=20)
    filename = os.path.join(output_dir, f"fig_bmc_slice_{turn:04.0f}")
    print(filename)
    plt.savefig(filename)
    plt.close("all")


# Set number of turns
nturns = cfg.bcm.nturns
turns = np.arange(nturns)

# Set shared profile coordinates
coords_t = np.arange(samp_per_turn) * t_step
coords_z = coords_t * (beta * speed_of_light)  # t [s] -> z [m]
coords_z = coords_z - np.median(coords_z)

# Extract turn-by-turn profiles
profiles = np.zeros((nturns, samp_per_turn))
for index, turn in enumerate(turns):
    lo = turn * samp_per_turn
    hi = lo + samp_per_turn
    profiles[index, :] = np.copy(waveform[lo:hi, 1])

# Force positive signals by subtracting global min
profiles = np.array(profiles)
profiles = profiles + np.min(profiles)

# Create xarray
profiles = xr.DataArray(profiles, dims=["turn", "z"], coords=[turns, coords_z])
profiles["z"].attrs["units"] = "meters"

print("profiles:")
print(profiles)


# Save data
# --------------------------------------------------------------------------------------

filename = os.path.join(output_dir, "profiles.nc")
print(filename)
profiles.to_netcdf(filename)
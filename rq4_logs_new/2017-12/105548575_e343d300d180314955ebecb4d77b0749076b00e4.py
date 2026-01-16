#!/usr/bin/env python3

import chemkin.chemkin
import numpy as np

reader = chemkin.chemkin.XMLReader("rxns_reversible.xml")
reaction_system = reader.get_reaction_systems()[0]

# Some example concentrations
# 10 molecules of each starting species
abundances = np.array([10]*8)
# Converted to concentration for volume of 1e-15
concentrations = abundances/6.02e23/1e-15

# Calculate initial reaction rates for temperature of 800 K
reaction_rate = reaction_system.calculate_reaction_rate(
    concentrations, 800)
print("Inital reaction rates:")
print(reaction_rate)

# Deterministic simulation
deterministic_simulator = reaction_system.setup_reaction_simulator(
    'deterministic', concentrations, 400, [0, 1e-4], dt=1e-7)
print("Running deterministic simulation")
deterministic_simulator.simulate(epsilon=1e-9)

# Stochastic simulation
stochastic_simulator = reaction_system.setup_reaction_simulator(
    'stochastic', abundances, 400, [0, 1e-4], 1e-15)
print("Running stochastic simulation")
stochastic_simulator.simulate()

# Plot abundance/concentrationv versus time of deterministic
#   simulation and stochastic simulation
deterministic_simulator.plot_simulation(show=False,
                                        title="Deterministic Simulation")
stochastic_simulator.plot_simulation(title="Stochastic Simulation")
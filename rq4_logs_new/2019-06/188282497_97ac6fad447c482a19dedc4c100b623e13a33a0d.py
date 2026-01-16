#!/usr/bin/env python3
import pytudat

# set up default simulation
simulation = pytudat.tudatConfig(
        "Hypersat",
        86400,
        pytudat.OrbitalElements(7.5E6,0.1,1.4888,4.1137,0.4084,2.4412))

# add effect
simulation.propagators[0].accelerations.Hypersat.Earth.append(pytudat.Empty())
simulation.propagators[0].accelerations.Hypersat.Earth[1].type="sphericalHarmonicGravity"
simulation.propagators[0].accelerations.Hypersat.Earth[1].maximumOrder=3
simulation.propagators[0].accelerations.Hypersat.Earth[1].maximumDegree=3.

# run the simulation and print the results
results = pytudat.run(simulation)
print(results)







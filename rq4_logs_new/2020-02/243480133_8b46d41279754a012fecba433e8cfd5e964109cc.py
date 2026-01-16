from simtk.openmm.app import *
from simtk.openmm import *
from simtk.unit import *
from sys import stdout

def annealing(integrator, simulation, target_temperature):
    temperature_list = list(range(0, target_temperature, 3))
    temperature_list.append(target_temperature)
    for temperature in temperature_list:
        integrator.setTemperature(temperature*kelvin)
        simulation.step(100)

pdb = PDBFile('model/alat.pdb')
positions = pdb.getPositions(asNumpy=True)
forcefield = ForceField('amber14-all.xml', 'amber14/tip3pfb.xml')
system = forcefield.createSystem(pdb.topology, nonbondedMethod=NoCutoff, nonbondedCutoff=1*nanometer, constraints=HBonds)

"""
バイアスポテンシャルをかける
読み込ませたpdbファイルの位置に止まらせるような力を加える
"""
force = CustomExternalForce('k*((x-x0)^2+(y-y0)^2+(z-z0)^2)')
force.addGlobalParameter("k", 10.0)
x0_id = force.addPerParticleParameter("x0")
y0_id = force.addPerParticleParameter("y0")
z0_id = force.addPerParticleParameter("z0")
system.addForce(force)
for i in range(system.getNumParticles()):
    force.addParticle(i, positions[i])

integrator = LangevinIntegrator(300*kelvin, 1/picosecond, 0.002*picoseconds)
simulation = Simulation(pdb.topology, system, integrator)
simulation.context.setPositions(pdb.positions)
simulation.minimizeEnergy()

annealing(integrator, simulation, 300)

simulation.reporters.append(PDBReporter('output.pdb', 1000))
simulation.reporters.append(StateDataReporter(stdout, 1000, step=True,
        potentialEnergy=True, temperature=True))
simulation.step(10000)
import discretisedfield as df
import micromagneticmodel as mm
import oommfc as oc

# Geometry
lx = 1000e-9  # x dimension of the sample(m)
ly = 1000e-9  # y dimension of the sample (m)
lz = 9e-9  # sample thickness (m)

# Material (permalloy) parameters
Ms = 8e5  # saturation magnetisation (A/m)
A = 1.3e-11  # exchange energy constant (J/m)

# Dynamics (LLG equation) parameters
gamma0 = 2.211e5  # gyromagnetic ratio (m/As)
alpha = 0.02  # Gilbert damping

system = mm.System(name='stdprob4_1000x1000')

cell = (5e-9, 5e-9, 3e-9)  # mesh discretisation (m)
mesh = df.Mesh(p1=(0, 0, 0), p2=(lx, ly, lz), cell=cell)  # Create a mesh object.

mesh.k3d()

system.energy = mm.Exchange(A=A) + mm.Demag()

system.dynamics = mm.Precession(gamma0=gamma0) + mm.Damping(alpha=alpha)

system.m = df.Field(mesh, dim=3, value=(1, 0.25, 0.1), norm=Ms)

md = oc.MinDriver()  # create energy minimisation driver
md.drive(system)  # minimise the system's energy

from matplotlib import pyplot as plt

# x-component
system.m.x.plane('z').mpl()
plt.savefig("x1.png", dpi=300)
plt.close()

# y-component
system.m.y.plane('z').mpl()
plt.savefig("y1.png", dpi=300)
plt.close()





# make figure larger
fig, ax = plt.subplots(figsize=(20, 5))

# plot vectors on grid of 20 x 5 over the numerical resulotion
system.m.plane('z', n=(20, 5)).mpl.vector(ax=ax)

# add colouring for mx-component to this plot
system.m.x.plane('z').mpl.scalar(ax=ax, cmap='magma')
plt.savefig("vector1.png", dpi=300)
plt.close()

print('The average magnetisation is {}.'.format(system.m.average))

print('The magnetisation at the mesh centre {}\nis {}.'.format(
        system.m.mesh.region.centre, system.m(system.m.mesh.region.centre)))



# Add Zeeman energy term to the Hamiltonian
H1 = (-24.6e-3/mm.consts.mu0, 4.3e-3/mm.consts.mu0, 0.0)
system.energy += mm.Zeeman(H=H1)

t = 50e-9  # simulation time (s)
n = 10000  # number of data saving steps



td = oc.TimeDriver()  # create time driver
td.drive(system, t=t, n=n)  # drive the system


myplot = system.table.data.plot('t', 'my')
plt.savefig("M_y.png", dpi=300)
plt.close()

# vectors

# make figure larger
fig, ax = plt.subplots(figsize=(20, 5))

# plot vectors on grid of 20 x 5 over the numerical resulotion
system.m.plane('z', n=(20, 5)).mpl.vector(ax=ax)

# add colouring for mx-component to this plot
system.m.x.plane('z').mpl.scalar(ax=ax, cmap='magma')
plt.savefig("vector2.png", dpi=300)
plt.close()
#Load python packages
from vmd import evaltcl
import numpy as np
from molecule import load
from atomsel import atomsel
import molecule
import math

#Load nanotube package in VMD
evaltcl('package require nanotube')

#make a 5nm x 5nm grpahene sheet
NT=evaltcl('graphene -lx 5 -ly 5 -type zigzag -nlayers 1 -b 1 -a 0 -d 0 -i 0')

#Figure out the correct unit cell size and set it so we can get the correct PBC
all=atomsel()
CCdist=1.42
X=all.get('x')
Y=all.get('y')
pbc_x = np.max(X) - np.min(X) + CCdist
pbc_y = np.max(Y) - np.min(Y) + CCdist * np.sin(60./360.*math.pi*2)
pbc = [ pbc_x, pbc_y, 20 ]

#We need another VMD package to set the periodic boundary conditions / unit cell
evaltcl('package require pbctools')

#We need to use the VMD packag pbctools, which doesn't have a python interface,
#so we explicitly do this by hand with evaltcl
evaltcl('pbc set {%f %f %f}'%(pbc[0],pbc[1],pbc[2]))

evaltcl('package require topotools')


#Helper python function to turn a python list into something tcl can understand
def list2tcl(listin):
    strout='{'
    for a in listin:
        strout+= (' ' + str(a))
    strout+='}'
    return strout


#Add a bunch of toy sphere molecules
z0=3.33

#two spheres in x direction
for x in np.linspace(5,45,1):
    #two sphere in y direction
    for y in np.linspace(5,45,1):
        for z0 in [3.]:

            #load the O atom from O.pdb
            O=load('pdb','O.pdb')

            #Move the O atom into the right place
            all=atomsel()
            all.moveby([x,y,z0])
            
            #Set resid to 0 (for now, doesn't matter, just needs to be something
            # so that the simulation program isn't unhappy
            all=atomsel()
            all.set('resid',0)
   

#So far we have 26 molecfules (1 graphene, 25 O).  We need to combine them into one thing
#so that we can treat them together. We use topotools to do this
combined=evaltcl('::TopoTools::mergemols %s'%list2tcl(molecule.listall()))


import animate

#Set the segname of everything to CNT (doesn't matter now, just to make sim happy)
all=atomsel()
all.set('segname','CNT')
all.set('resname','CNT')

#Set the occupacy of everything to 0
all.set('occupancy',0)

#Set the occupancy of just the CA atoms to 1, which we'll use as a flag to keep them fixed
all=atomsel('type CA')
all.set('occupancy',1)
all=atomsel('type O')
all.set('beta',1)
all=atomsel('type CA')
all.set('beta',2)
all=atomsel()
all.moveby(-np.array(all.center()))
#all.moveby((0.,0.,-8))
all.write('pdb','cnt_chains.pdb')
all.write('psf','cnt_chains.psf')

all=atomsel('type CA')
all.set('z',0.)
all.set('y',0)
all=atomsel('type O')

all.set('y',1.)
all.set('z',all.get('z'))
all=atomsel()
all.write('pdb','cnt_chains_cons.pdb')

for a in molecule.listall():
    molecule.delete(a)

#combined=evaltcl('package require autoionize')
#evaltcl('autoionize -psf test.psf -pdb test.pdb -neutralize')
exit
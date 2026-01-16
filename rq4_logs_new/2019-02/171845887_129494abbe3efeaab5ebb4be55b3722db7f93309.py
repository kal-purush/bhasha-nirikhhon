from pyinvariant import *
import time
import numpy as np
from pprint import pprint

# Define the search space
space = IntervalVector([[-20, 20],[-20,20]])

# Create the Subpaving structure
subPaving = SmartSubPaving(space)

# Create the Domain
dom = Domain(subPaving, FULL_DOOR)
dom.set_border_path_in(False)
dom.set_border_path_out(False)


# Create the Dynamics
f = Function("x[2]", "(-x[1],x[0])") 
dyn = DynamicsFunction(f, FWD_BWD)

# Create the Maze associated with the Domain and the dynamics
maze = Maze(dom, dyn)

# Contract the system
for i in range(12): # Number of bisections
	print(i)
	subPaving.bisect()
	maze.contract()
pprint(dir(maze))
# Visualization
visu = VibesMaze(" Invariant", maze)
visu.setProperties(0,0,512,512)
visu.show()
visu.drawCircle(0, 0, 0.3, "black[red]");
from forces import Forces
from shapes.circle import Circle
import numpy as np


vf1_saved = np.array((71,-16))

Circle1 = Circle(10.0, x=50, y=210, x_vel=42,y_vel=13)

Circle2 = Circle(10.0, x=20, y=240, x_vel=100,y_vel=13)

f1 = Forces()

vf1,vf2 = f1.calculate_final_collision_velocities(Circle1,Circle2)

def test_vf1_collision_calc():
    """
    Check to see if the collision calculation answer
    is different...
    It should be constant so a change in answer indicates 
    a likely unintended change in the code."""
    assert np.array_equal(vf1, vf1_saved) == True
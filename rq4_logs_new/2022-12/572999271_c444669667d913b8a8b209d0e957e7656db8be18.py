import numpy as np
from simulation import Simulation
from shapes.circle import Circle
from environment import Environment
from forces import Forces


# Tests obj-obj collisions with gravity forces

radius = 10
screen_size = 300

circles = []
for i in range(5):
    circles.append(Circle(10.0, x=50 * (i + 1), y=250, x_vel=1,
                   y_vel=3 * (i + 1), color=(0, 100, 0)))

list_of_shapes = circles
env_data = np.zeros((6, 6))
env1 = Environment(env_data)
force1 = Forces(gravity=True)

sim = Simulation(
    screen_width=screen_size,
    screen_height=screen_size,
    sim_time=0,
    shapes=list_of_shapes,
    environment=env1,
    forces=force1,
    render=True
)


final_positions = sim.run()





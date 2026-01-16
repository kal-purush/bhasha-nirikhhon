from simulator import Simulator
import math

simulator = Simulator(
                    velocity=1.0440306508910566,angle=math.radians(73.30075576600639),spin=-16,
                    CoSF=0.2,VRC=0.9,HRC=0.3,Radius=0.04,D=0,AMC=0.6
                    )
print(simulator.simulate_sliding()[1])
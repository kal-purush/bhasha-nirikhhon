#
"""
Queuing System Discrete Event Simulation in Python (Process interaction)
Reference: https://www.youtube.com/watch?v=eSNfC-HOl44&list=PLJES8iAtf6Dvr7D6L0hZPramTpckN8GEU&index=3
"""
import simpy
import numpy as np

def generate_i

def cafe_run(env):
  while True:
    yield env.timeout()

env = simpy.Environment()

env.run(until=10)
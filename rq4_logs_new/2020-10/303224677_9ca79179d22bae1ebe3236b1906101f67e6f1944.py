import numpy as np

from bodies.body import Body


class Manifold:
    def __init__(self, a: Body, b: Body, normal_vector: np.array):
        self.a = a
        self.b = b
        self.normal_vector = normal_vector
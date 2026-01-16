import math
from math import log2
from scipy.stats import qmc
from shapely import Polygon, Point

from Utils.Circle import Circle


def sobol(P: Polygon,
          n_points: int,
          radius: float = 1):
    sampler = qmc.Sobol(d=2, scramble=False)
    sample = sampler.random_base2(m=math.ceil(log2(n_points)))
    (minx, miny, maxx, maxy) = P.bounds
    d = max(maxx - minx, maxy - miny)  # Во сколько раз растянем сетку.
    sample = [Point(minx + d * p[0], miny + d * p[1]) for p in sample]
    return [Circle(Point(p), radius) for p in sample if P.contains(p)]
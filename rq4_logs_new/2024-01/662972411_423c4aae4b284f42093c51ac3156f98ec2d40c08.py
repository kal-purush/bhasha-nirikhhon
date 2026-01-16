import math
import random
import time

from Algorithms.Halton.Halton import halton
from shapely import Polygon, Point
import matplotlib.pyplot as plt

from Algorithms.NBodies.GravityFunctions import smooth_gravity_on_region_with_sign
from Algorithms.NBodies.RungeKuttaAlgorithm import RungeKuttaAlgorithm
from Examples.polygons import polygons_dict
from Utils.drawing import draw_polygon, draw_circles

R = 1
P = polygons_dict['P9']
(minx, miny, maxx, maxy) = P.bounds
P_described = Polygon([Point(minx - R, miny - R), Point(maxx + R, miny - R), Point(maxx + R, maxy + R), Point(minx - R, maxy + R)])

t0 = time.perf_counter()
centers = halton(
    P=P_described,
    n_points=4,
    p1=2,
    p2=3,
    start=1,
    step=1,
)

t1 = time.perf_counter()
rk_alg = RungeKuttaAlgorithm(centers, R)
rk_alg.set_params(
    STOP_RADIUS=1.5 * R,
    TIME_STOP=50,
    gravity=smooth_gravity_on_region_with_sign
)
rk_alg.run_algorithm()
ans = rk_alg.get_result()
#ans = list(filter(lambda c: P.contains(c.center), ans))

t2 = time.perf_counter()

print(f'Halton time: {t1 - t0} sec.')
print(f'RK time: {t2 - t1} sec.')
print(f'Elapsed time: {t2 - t0} sec.')

ax = plt.gca()
ax.set_aspect('equal', adjustable='box')

draw_polygon(ax, P)
draw_circles(ax, ans)

plt.show()

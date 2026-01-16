import numpy as np
from shapely import Point, Polygon, LineString
import math

from shapely.ops import unary_union

from Algorithms.Hexagonal.hexagonal_coverings import hexagonal
from Utils.Circle import Circle


class HexagonalAlgorithm:
    def __init__(self, P: Polygon, radius: float):
        self.P = P
        self.radius = radius
        (minx, miny, maxx, maxy) = P.bounds
        self.P_described = Polygon([Point(minx - radius, miny - radius),
                                    Point(maxx + radius, miny - radius),
                                    Point(maxx + radius, maxy + radius),
                                    Point(minx - radius, maxy + radius)])

    def set_params(self,
                   RESOLUTION: int=5,
                   ALPHA_RESOLUTION: int=5,
                   EPS: float = 1e-3,
                   hex_alg=hexagonal,
                   REMOVE_REDUNDANT=True):
        self.RESOLUTION = RESOLUTION
        self.ALPHA_RESOLUTION = ALPHA_RESOLUTION
        self.EPS = EPS,
        self.hex_alg = hex_alg
        self.REMOVE_REDUNDANT = REMOVE_REDUNDANT

    def run_algorithm(self):
        self.best_ops = (0, 0, 0)
        self.best_val = float('inf')

        step_alpha = math.pi / 3 / self.ALPHA_RESOLUTION
        step_x = self.radius / self.RESOLUTION
        step_y = self.radius / self.RESOLUTION
        alpha = 0
        while alpha < math.pi / 3:
            x = 0
            while x < self.radius:
                y = 0
                while y < self.radius:
                    S = Point(x, y)

                    important = self.get_important(S, alpha)

                    if len(important) < self.best_val:
                        self.best_val = len(important)
                        self.best_ops = (x, y, alpha)
                    y += step_y
                x += step_x
            alpha += step_alpha

    def get_important(self, S, alpha):
        outer_grid = self.hex_alg(self.P_described, S, self.radius, alpha)

        if not self.REMOVE_REDUNDANT:
            return [Circle(c, self.radius) for c in outer_grid]

        # Оставим только те круги, которые вносят значительный вклад в покрытие.
        inside = [Circle(c, self.radius) for c in outer_grid if self.P.contains(c)]
        outside = [Circle(c, self.radius) for c in outer_grid if not self.P.contains(c)]
        union = unary_union([c.polygon for c in inside])

        outside.sort(key=
                     lambda c: self.P.intersection(c.polygon).area,
                     reverse=True)
        for c in outside:
            c_inside_polygon = self.P.intersection(c.polygon)
            united = unary_union([union, c_inside_polygon])
            if abs(united.area - union.area) > self.EPS:
                inside.append(c)
                union = unary_union([union, c_inside_polygon])
        return inside

    def get_result(self, ):
        return self.get_important(Point(self.best_ops[0], self.best_ops[1]), self.best_ops[2])
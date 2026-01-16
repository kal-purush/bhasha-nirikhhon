import numpy as np
from shapely import Polygon, Point

from Algorithms.NBodies.GravityFunctions import smooth_gravity_with_sign
from Utils.Circle import Circle


class RungeKuttaAlgorithm:
    def __init__(self, P: Polygon, radius: float, centers: list[Point]):
        self.P = P
        self.radius = radius
        self.centers = centers
        self.fixed = np.zeros(len(centers))

    def set_params(self,
                   fixed: list[int] = None,
                   G: float = 0.1,
                   STOP_RADIUS: float = None,
                   TIME_START: float = 0,
                   TIME_STEP: float = 0.5,
                   gravity: 'gravity function' = smooth_gravity_with_sign):
        if fixed is not None:
            self.fixed = fixed
        self.G = G

        if STOP_RADIUS is None:
            self.STOP_RADIUS = self.radius
        else:
            self.STOP_RADIUS = STOP_RADIUS

        self.TIME_START = TIME_START
        self.TIME_STEP = TIME_STEP
        self.gravity = gravity

    def __gravity(self, t, y: np.array):
        """
        Вспомогательная функция для передачи в метод Рунге-Кутта.
        Вычисляет дискретную производную в точке y в момент времени t для каждого тела.
        :param t: Момент времени, для которого происходит вычисление.
        :param y: Записанные в строку координаты тел: [x0, y0, x1, y1, x2, y2, ... ]
        :return: Дискретную производную по каждой переменной: [dx0, dy0, dx1, dy1, dx2, dy2, ... ]
        """
        n = y.shape[0]
        fn = np.zeros(n)
        # Считаем попарную силу взаимодействия с каждым другим телом.
        for i in np.arange(0, n, 2):
            A = np.array([y[i], y[i + 1]])
            for j in np.arange(0, n, 2):
                B = np.array([y[j], y[j + 1]])
                fn[i:i + 2] += smooth_gravity_with_sign(A,
                                               B,
                                               G=self.G,
                                               STOP_RADIUS=self.STOP_RADIUS)
        return fn

    def run_algorithm(self):
        pass

    def get_result(self):
        return [Circle(Point(c), self.radius) for c in self.centers]
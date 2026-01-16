from random import randint

import numpy as np
from matplotlib import pyplot as plt
from celluloid import Camera

from Algorithms.NBodies.GravityFunctions import smooth_gravity_with_sign


def Euler(points: np.array,
          t: float,
          h: float,
          f: "function - law of interaction") -> np.array:
    t0 = 0
    n = len(points)
    while t0 < t:
        fn = np.zeros((n, 2))
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                fn[i] += h * f(points[i], points[j])
        points += fn

        t0 += h

    return points


if __name__ == '__main__':
    points = np.array([[randint(-5, 5), randint(-5, 5)] for i in range(10)]).astype('float')
    fig = plt.figure()
    ax = plt.axes(xlim=(-6, 6), ylim=(-6, 6))
    camera = Camera(fig)

    for i in range(200):
        ax.scatter(points[:, 0], points[:, 1])

        camera.snap()
        points = Euler(points, 0.01, 0.01, smooth_gravity_with_sign) #cut_gravity_with_sign

    animation = camera.animate()
    animation.save('points.gif', writer='imagemagick')
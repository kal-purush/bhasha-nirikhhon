import numpy as np

from Lagrange import Lagrange

left = int(input('Enter left border: '))
right = int(input('Enter right border: '))
n = int(input('Enter number of points: '))


def func(x):
    return np.sin(0.72*(x ** 3) + 0.054 * x)


lagrange = Lagrange(left, right, n, func)
lagrange.calculate()
lagrange.plots()
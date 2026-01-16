import time

import numpy as np
from matplotlib import pyplot as plt
from Algorithms.BranchesAndBounds.BranchesAndBounds import bnb
from Algorithms.Hexagonal.hexagonal import HexagonalAlgorithm
from Algorithms.NBodies.RungeKuttaAlgorithm import RungeKuttaAlgorithm
from Examples.polygons import polygons_dict
from Utils.drawing import draw_polygon, draw_circles
from Utils.layering import get_layers

P = polygons_dict['P3']
R = 1  # Радиус.
INNER_BOUND = 2  # Начиная с этого слоя по удалению от внешних границ многоугольника круг считается внутренним.

# Построим покрытие шестиугольной сеткой.
t0 = time.perf_counter()

hex_alg = HexagonalAlgorithm(P, R)  # Укажем данные.
hex_alg.set_params()  # Укажем параметры решения.
hex_alg.run_algorithm()  # Запустим алгоритм.
hex_ans = hex_alg.get_result()  # Получим результат - list[Circle].
t1 = time.perf_counter()

# Разложим круги по уровням дальности до края многоугольника
layers = get_layers(P, hex_ans)

# Выделим "внутренние" круги.
inners = np.zeros(len(layers))
inners[layers >= INNER_BOUND] = 1

t2 = time.perf_counter()

# Починим методом ветвей и границ
bnb_grid = bnb(P,
               hex_ans,
               max_iterations=10,
               is_repaired=True)  # TODO: метод ветвей и границ только для некоторых вершин (параметр fixed)


t3 = time.perf_counter()

# Придвинем поближе внешние круги с помощью метода Рунге-Кутта.
rk_alg = RungeKuttaAlgorithm(
    [c.center for c in bnb_grid],
    R)
rk_alg.set_params(
    fixed=inners,  # TODO: другие параметры???,
    STOP_RADIUS=1.5 * R,
    TIME_STOP=20
)
rk_alg.run_algorithm()
rk_ans = rk_alg.get_result()

t4 = time.perf_counter()

# Выведем числовые результаты работы.
print(f'Hex creation time: {t1 - t0} sec.')
print(f'Get layers time: {t2 - t1} sec.')
print(f'BnB time: {t3 - t2} sec.')
print(f'Runge-Kutta time: {t4 - t3} sec.')
print(f'Elapsed time: {t4 - t0} sec.')
print(f'Before BnB result: {len(hex_ans)} circles')
print(f'BnB results:  {len(bnb_grid)} circles.')

fig, ax = plt.subplots(nrows=1, ncols=3)
# Отрисуем результат построения сетки.
ax[0].set_aspect('equal', adjustable='box')
draw_polygon(ax[0], P)
draw_circles(ax[0], hex_ans)

# Отрисуем результат после запуска метода Рунге-Кутта
ax[1].set_aspect('equal', adjustable='box')
draw_polygon(ax[1], P)
draw_circles(ax[1], bnb_grid)

# Отрисуем результат после запуска метода ветвей и границ.
ax[2].set_aspect('equal', adjustable='box')
draw_polygon(ax[2], P)
draw_circles(ax[2], rk_ans)

plt.show()

print('aboba')
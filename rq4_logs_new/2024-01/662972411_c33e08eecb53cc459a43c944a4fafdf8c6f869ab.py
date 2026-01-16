import time

import numpy as np
from matplotlib import pyplot as plt
from Algorithms.BranchesAndBounds.FlexibleBnBAlgorithm import FlexibleBnBAlgorithm
from Algorithms.BranchesAndBounds.Loggers.BnBAnimationLogger import BnBAnimationLogger
from Algorithms.BranchesAndBounds.Loggers.BnBMetricLogger import BnBMetricLogger
from Algorithms.BranchesAndBounds.ParamsClasses.ExpBnBParams import ExpBnBParams
from Algorithms.Hexagonal.HexagonalAlgorithm import HexagonalAlgorithm
from Algorithms.Hexagonal.hexagonal_coverings import hexagonal_np
from Examples.polygons import polygons_dict
from Utils.drawing import draw_polygon, draw_circles
from Utils.layering import get_layers


def main(poly_name='P1', show_plt=False):
    print(f"\n\nProcessing {poly_name}")

    P = polygons_dict[poly_name]
    R = 1  # Радиус.
    INNER_BOUND = 2  # Начиная с этого слоя по удалению от внешних границ многоугольника круг считается внутренним.

    # Построим покрытие шестиугольной сеткой.
    t0 = time.perf_counter()

    hex_alg = HexagonalAlgorithm(P, R)  # Укажем данные.
    hex_alg.set_params(
        hex_alg=hexagonal_np,
        REMOVE_REDUNDANT=False
    )  # Укажем параметры решения.
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
    bnb_alg = FlexibleBnBAlgorithm(P, hex_ans)
    # Запустим алгоритм с приоритетом на удаление кругов.
    bnb_alg.set_params(
        max_iterations=10,
        params=ExpBnBParams(
            P,
            len(hex_ans),
            CIRCLE_COUNT_W=0.1,
            animation_logger=BnBAnimationLogger(),  # TODO: отключить автоматический ресет логгеров
            metric_logger=BnBMetricLogger(),
            MOVE_SCHEDULE=(lambda x: 0.985 * x)),  # TODO: 0.985
        fixed=list(inners)
    )
    bnb_alg.run_algorithm()

    # Запустим алгоритм с приоритетом на перемещение кругов.
    bnb_alg.params.CIRCLE_COUNT_W = 0.005
    bnb_alg.set_params(
        max_iterations=30)
    bnb_alg.run_algorithm()
    bnb_grid = bnb_alg.get_result()

    # Выгрузим логи
    if bnb_alg.params.animation_logger:
        bnb_alg.params.animation_logger.save_log(f'exp_bnb/0.985/{poly_name}.gif')
    if bnb_alg.params.metric_logger:
        bnb_alg.params.metric_logger.save_log(f'exp_bnb/0.985/{poly_name}_log.png')

    t3 = time.perf_counter()

    # Выведем числовые результаты работы.
    print(f'Hex creation time: {t1 - t0} sec.')
    print(f'Get layers time: {t2 - t1} sec.')
    print(f'BnB time: {t3 - t2} sec.')
    print(f'Elapsed time: {t3 - t0} sec.')
    print(f'Before BnB result: {len(hex_ans)} circles')
    print(f'BnB results:  {len(bnb_grid)} circles.')

    fig, ax = plt.subplots(nrows=1, ncols=2)
    # Отрисуем результат построения сетки.
    ax[0].set_aspect('equal', adjustable='box')
    draw_polygon(ax[0], P)
    draw_circles(ax[0], hex_ans)

    # Отрисуем результат после запуска метода ветвей и границ.
    ax[1].set_aspect('equal', adjustable='box')
    draw_polygon(ax[1], P)
    draw_circles(ax[1], bnb_grid)

    if show_plt:
        plt.show()


if __name__ == '__main__':
    for poly_name in polygons_dict.keys():
        main(poly_name)
    # main('P1', show_plt=True)
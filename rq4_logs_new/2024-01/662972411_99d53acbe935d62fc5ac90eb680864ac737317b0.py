from celluloid import Camera
from matplotlib import pyplot as plt

from Algorithms.BranchesAndBounds.Branch import Branch
from Utils.drawing import draw_circles, draw_polygon


class BnBAnimationLogger:
    def __init__(self, xlim=(-6, 6), ylim=(-6, 6)):
        self.reset(xlim, ylim)

    def reset(self, xlim=(-6, 6), ylim=(-6, 6)):
        """
        Приводит логгер в готовое состояние.

        :param xlim: Ограничения для абсциссы графика.
        :param ylim: Ограничения для ординаты графика.
        """
        fig = plt.figure()
        self.ax = plt.axes(xlim=xlim, ylim=ylim)
        self.ax.set_aspect('equal', adjustable='box')
        self.camera = Camera(fig)

    def snap(self, branch: Branch):
        """
        Делает снимок и добавляет его к анимации.
        """
        draw_polygon(self.ax, branch.polygon)
        draw_circles(self.ax, branch.circles)
        self.camera.snap()

    def save_log(self, path='log.gif'):
        """
        Сохраняет собранную на данный момент анимацию.
        """
        animation = self.camera.animate()
        animation.save(path, writer='imagemagick')
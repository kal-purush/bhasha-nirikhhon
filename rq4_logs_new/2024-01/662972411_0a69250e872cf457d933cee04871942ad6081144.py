import numpy as np
from matplotlib import pyplot as plt


class BnBMetricLogger:
    def __init__(self):
        self.metric_array = []  # Массив метрики лучшей ветви.
        self.parts_arrays = []  # Массив составляющих частей этой метрики.
        self.temp_metric = []  # Вспомогательный массив метрик ветвей, чтобы выбрать лучшую ветвь.
        self.temp_parts = []  # Соответствующий ему набор частей значений метрики разных ветвей.

    def reset(self, parts_names: list[str] = None):
        self.parts_names = parts_names  # Названия составляющих частей метрики для построения графика.
        self.metric_array = []
        self.parts_arrays = []
        self.temp_metric = []
        self.temp_parts = []

    def add_info(self,
                 metric: float,
                 parts: tuple):
        """
        Добавляет информацию о метрике отдельной ветви.

        :param metric: Значение метрики.
        :param parts: Значение её составных частей.
        """

        self.temp_metric.append(metric)

        k = len(parts)
        if len(self.temp_parts) == 0:
            self.temp_parts = [[] for _ in range(k)]
        for i in range(k):
            self.temp_parts[i].append(parts[i])

    def harden(self):
        """
        Выбирает лучшую ветвь из загруженных методом add_info.
        Сохраняет её метрику для построения графиков.
        """
        best_ind = np.argmax(self.temp_metric)
        self.metric_array.append(self.temp_metric[best_ind])

        k = len(self.temp_parts)
        if len(self.parts_arrays) == 0:
            self.parts_arrays = [[] for _ in range(k)]
        for i in range(k):
            self.parts_arrays[i].append(self.temp_parts[i][best_ind])

        self.temp_metric = []
        self.temp_parts = []

    def save_log(self, path='metric.png'):
        """
        Строит графики по метрикам и сохраняет по указанному пути.

        :param path: Путь для сохранения графиков.
        """
        n = 1 + len(self.parts_arrays)
        fig, ax = plt.subplots(nrows=n, ncols=1)

        fig.set_figheight(15)
        plt.subplots_adjust(hspace=2)
        x = np.arange(0, len(self.metric_array))

        ax[0].set_title('metric')
        ax[0].plot(x, self.metric_array)

        for i in range(1, n):
            ax[i].plot(x, self.parts_arrays[i - 1])
            if self.parts_names:
                ax[i].set_title(self.parts_names[i - 1])

        fig.savefig(path)
        plt.close(fig)
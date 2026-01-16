"""
1. Создать класс TrafficLight (светофор) и определить у него один атрибут
color (цвет) и метод running (запуск). Атрибут реализовать как приватный.
В рамках метода реализовать переключение светофора в режимы: красный, желтый,
зеленый. Продолжительность первого состояния (красный) составляет 7 секунд,
второго (желтый) — 2 секунды, третьего (зеленый) — на ваше усмотрение.
Переключение между режимами должно осуществляться только в указанном порядке
(красный, желтый, зеленый). Проверить работу примера, создав экземпляр и вызвав
описанный метод.
"""
from time import sleep
from itertools import cycle


class TrafficLight:

    __color = cycle([0, 1, 2])
    count = [7, 2, 5]  # таймеры для красного, жёлтого и зелёного сигналов, сек

    def get_color(self):
        return self.__color

    def running(self):
        cur_mode = next(self.__color)
        if cur_mode == 0:  # красный
            print('\nКрасный', end='')
            for _ in range(self.count[cur_mode]):
                sleep(1.0)
                print('.', end='')
        elif cur_mode == 1:  # жёлтый
            print('\nЖёлтый', end='')
            for _ in range(self.count[cur_mode]):
                sleep(1.0)
                print('.', end='')
        elif cur_mode == 2:  # зелёный
            print('\nЗелёный', end='')
            for _ in range(self.count[cur_mode]):
                sleep(1.0)
                print('.', end='')
            print('\n', '_' * 8)
        self.running()


t_light1 = TrafficLight()
try:
    print('Запуск светофора...')
    t_light1.running()
except KeyboardInterrupt:
    print('Завершаем работу.')
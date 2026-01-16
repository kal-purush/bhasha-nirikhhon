# Создать класс TrafficLight (светофор) и определить у него один атрибут color (цвет) и метод running (запуск).
# Атрибут реализовать как приватный. В рамках метода реализовать переключение светофора в режимы: красный, желтый, зеленый.
# Продолжительность первого состояния (красный) составляет 7 секунд, второго (желтый) — 2 секунды, третьего (зеленый) — на ваше усмотрение.
# Переключение между режимами должно осуществляться только в указанном порядке (красный, желтый, зеленый).
# Проверить работу примера, создав экземпляр и вызвав описанный метод.
# Задачу можно усложнить, реализовав проверку порядка режимов, и при его нарушении выводить соответствующее сообщение и завершать скрипт.

from termcolor import colored, cprint
import time
#import datetime

class TrafficLight:
    __delay = {'red': 7, 'yellow': 2, 'green': 4}
    __colors = ('red', 'yellow', 'green')

    __is_reverse = False
    __color = 'red'

    def __next_color(self):
        # Усложняем алгоритм определения следующего цвета в соответствии с постановкой на уроке
        if str(self.__color) == 'red':
            self.__color = 'yellow'
        elif str(self.__color) == 'yellow' and (not self.__is_reverse):
            self.__color = 'green'
            self.__is_reverse = not self.__is_reverse
        elif str(self.__color) == 'yellow' and self.__is_reverse:
            self.__color = 'red'
            self.__is_reverse = not self.__is_reverse
        elif str(self.__color) == 'green':
            self.__color = 'yellow'

    def __show_color(self):
        cprint(self.__color, self.__color)
#        print(datetime.datetime.now())

    def __sleep(self):
        time.sleep(int(self.__delay.get(self.__color)))

    def running(self):
        while True:
            self.__show_color()
            self.__sleep()
            self.__next_color()


TrafficLight_1 = TrafficLight()
TrafficLight_1.running()
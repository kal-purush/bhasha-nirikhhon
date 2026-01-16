import time


class TrafficLights:
    __color = ''

    def print_obg(self):
        while True:
            self.__color = 'Красный'
            print(f'\r\033[31m{self.__color}', end='')
            time.sleep(7)
            self.__color = 'Желтый'
            print(f'\r\033[33m{self.__color}', end='')
            time.sleep(2)
            self.__color = 'Зеленый'
            print(f'\r\033[32m{self.__color}', end='')
            time.sleep(7)
            self.__color = 'Желтый'
            print(f'\r\033[33m{self.__color}', end='')
            time.sleep(2)


color_1 = TrafficLights()
color_1.print_obg()
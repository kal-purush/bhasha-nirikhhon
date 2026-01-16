# coding: UTF-8

# Задача - 1
# Опишите несколько классов TownCar, SportCar, WorkCar, PoliceCar
# У каждого класса должны быть следующие аттрибуты:
# speed, color, name, is_police - Булево значение.
# А так же несколько методов: go, stop, turn(direction) - которые должны сообщать,
#  о том что машина поехала, остановилась, повернула(куда)

class TownCar:
    def __init__(self, name, color, speed, is_police):
        self.name = name
        self.color = color
        self.speed = speed
        self.is_police = False

    def go(self):
        print("Машина {} поехала".format(self.name))

    def stop(self):
        print("Машина {} остановилась".format(self.name))

    def turn(self, direction):
        print("Машина {} повернула {}".format(self.name, direction))

class SportCar:
    def __init__(self, name, color, speed, is_police):
        self.name = name
        self.color = color
        self.speed = speed
        self.is_police = False

    def go(self):
        print("Машина {} поехала".format(self.name))

    def stop(self):
        print("Машина {} остановилась".format(self.name))

    def turn(self, direction):
        print("Машина {} повернула {}".format(self.name, direction))


class WorkCar:
    def __init__(self, name, color, speed, is_police):
        self.name = name
        self.color = color
        self.speed = speed
        self.is_police = False

    def go(self):
        print("Машина {} поехала".format(self.name))

    def stop(self):
        print("Машина {} остановилась".format(self.name))

    def turn(self, direction):
        print("Машина {} повернула {}".format(self.name, direction))


class PoliceCar:
    def __init__(self, name, color, speed, is_police):
        self.name = name
        self.color = color
        self.speed = speed
        self.is_police = True

    def go(self):
        print("Машина {} поехала".format(self.name))

    def stop(self):
        print("Машина {} остановилась".format(self.name))

    def turn(self, direction):
        print("Машина {} повернула {}".format(self.name, direction))



# Задача - 2
# Посмотрите на задачу-1 подумайте как выделить общие признаки классов
# в родительский и остальные просто наследовать от него.

class Car:
    def __init__(self, name, color, speed):
        self.name = name
        self.color = color
        self.speed = speed

    def go(self):
        print("Машина {} поехала".format(self.name))

    def stop(self):
        print("Машина {} остановилась".format(self.name))

    def turn(self, direction):
        print("Машина {} повернула {}".format(self.name, direction))

class TownCar(Car):

    def _init_(self, name, color, speed):
        super().__init__(name, color, speed)


class SportCar(Car):

    def _init_(self, name, color, speed):
        super().__init__(name, color, speed)


class WorkCar(Car):

    def _init_(self, name, color, speed):
        super().__init__(name, color, speed)


class PoliceCar(Car):

    def _init_(self, name, color, speed, is_police):
        super().__init__(name, color, speed)
        self.is_police = True



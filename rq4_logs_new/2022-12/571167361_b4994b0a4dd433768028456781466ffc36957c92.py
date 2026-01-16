class Animals:
    def __init__(self, speed, wool):
        self.speed = speed
        self.wool = wool

class Mammals(Animals):
    def __init__(self, speed, wool, flying, t_eating, t_movement):
        super().__init__(speed, wool)
        self.flying = flying
        self.t_eating = t_eating
        self.t_movement = t_movement

class Human(Mammals):
    def __init__(self, speed, wool, flying, t_eating, t_movement, name, surname, age, weight, height):
        super().__init__(speed, wool, flying, t_eating, t_movement)
        self.name = name
        self.surname = surname
        self.age = age
        self.weight = weight
        self.height = height

Peter = Human(5, 'No', 'Не умеет', 'Что угодно', 'Ноги', 'Пётр', 'Марцинкевич', 20, 80, 176)
print(Peter.name)

class Cat(Mammals):
    def __init__(self, speed, wool, flying, t_eating, t_movement, name, age, weight, sound):
        super().__init__(speed, wool, flying, t_eating, t_movement)
        self.name = name
        self.age = age
        self.weight = weight
        self.sound = sound

cat = Cat(20, 'Есть', 'Не умеет', 'Что угодно', 'Лапки', 'Мася', 3, 2, 'Мяу')
print(cat.name)   
print(cat.sound)

class Dog(Mammals):
    def __init__(self, speed, wool, flying, t_eating, t_movement, name, age, weight, sound):
        super().__init__(speed, wool, flying, t_eating, t_movement)
        self.name = name
        self.age = age
        self.weight = weight
        self.sound = sound

dog1 = Dog(20, 'Есть', 'Не умеет', 'Что угодно', 'Лапы', 'Пёсель', 3, 2, 'Гав')
print(dog1.name)   
print(dog1.sound)
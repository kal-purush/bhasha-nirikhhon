""" Создаем класс Цветы """
class Flowers:
    def __init__(self, freshness, color, stem_length, lifetime):
        self.freshness = freshness  # in days
        self.color = color
        self.stem_length = stem_length  # in cm
        self.lifetime = lifetime  # in days

""" Создаем подклассы для каждого вида цветка наследуясь от класса Цветы """
class Rose(Flowers):
    def __init__(self, freshness, color, stem_length, lifetime = 14, price = 25):
        super().__init__(freshness, color, stem_length, lifetime)
        self.price = price # in usd
        self.lifetime = lifetime
        self.life = lifetime - freshness
    def __str__(self):  # Меняем стиль вывода цветов на экран
        return f'{self.color} {self.__class__.__name__}'
    
class Tulip(Flowers):
    def __init__(self, freshness, color, stem_length, lifetime = 14, price = 15):
        super().__init__(freshness, color, stem_length, lifetime)
        self.price = price # in usd
        self.lifetime = lifetime
        self.life = lifetime - freshness
    def __str__(self):  # Меняем стиль вывода цветов на экран
        return f'{self.color} {self.__class__.__name__}'

class Violet(Flowers):
    def __init__(self, freshness, color, stem_length, lifetime = 14, price = 10):
        super().__init__(freshness, color, stem_length, lifetime)
        self.price = price # in usd
        self.lifetime = lifetime
        self.life = lifetime - freshness
    def __str__(self):  # Меняем стиль вывода цветов на экран
        return f'{self.color} {self.__class__.__name__}'

class Chamomile(Flowers):
    def __init__(self, freshness, color, stem_length, lifetime = 14, price = 5):
        super().__init__(freshness, color, stem_length, lifetime)
        self.price = price # in usd
        self.lifetime = lifetime
        self.life = lifetime - freshness
    def __str__(self):  # Меняем стиль вывода цветов на экран
        return f'{self.color} {self.__class__.__name__}'

""" Создаем класс Букет """
class Bouqet:
    def __init__(self, *flowers):
        self.flowers = flowers

    def get_flowers(self):  # выводит все цветы в букете
        bouqet = []
        for i in self.flowers:
            bouqet.append(i.__class__.__name__)
        return f'Букет {bouqet}'

    def dead_time(self):  # Среднее время жизни букета в днях
        count = 0
        dead_time = 0
        for i in self.flowers:
            dead_time += i.life
            count += 1
        dead_time = dead_time / count
        return f'Время жизни букета {round(dead_time)} дней'

    def sort_flowers_by_price(self):  # Сортировка букета по стоимости по возрастанию
        bouqet = []
        for i in self.flowers:
            bouqet.append(i)
        bouqet = sorted(bouqet, key = lambda flowers: flowers.price)
        sorted_bouqet = []
        for i in bouqet:
            sorted_bouqet.append(i.__class__.__name__)
        return f'Отсортировано по стоимости {sorted_bouqet}'

    def sort_flowers_by_freshness(self):  # Сортировка букета по свежести по возрастанию
        bouqet = []
        for i in self.flowers:
            bouqet.append(i)
        bouqet = sorted(bouqet, key = lambda flowers: flowers.freshness)
        sorted_bouqet = []
        for i in bouqet:
            sorted_bouqet.append(i.__class__.__name__)
        return f'Отсортировано по свежести {sorted_bouqet}'

    def sort_flowers_by_length(self):  # Сортировка букета по длине стебля по убыванию
        bouqet = []
        for i in self.flowers:
            bouqet.append(i)
        bouqet = sorted(bouqet, key = lambda flowers: flowers.stem_length, reverse = True)  # 
        sorted_bouqet = []
        for i in bouqet:
            sorted_bouqet.append(i.__class__.__name__)
        return f'Отсортировано по длине стебля {sorted_bouqet}'

    def __contains__(self, flower):  # Проверка на наличие в букете определенного цветка
        return flower in self.flowers

    def __getitem__(self, index):  # Вывод цветка из букета по индексу
        return self.flowers[index]

    def __iter__(self):  # Поочередный вывод цветков в букете
        return iter(self.flowers)

    def find_flower_by_freshness(self, freshness):  # Поиск цветка по свежести
        result = []
        for flower in self.flowers:
            if flower.freshness == freshness:
                result.append(flower.__class__.__name__)
        return f'Найдены цветы со свежестью в {freshness} дней: {result}'

    def find_flower_by_stem_length(self, stem_length):  # Поиск цветка по длине стебля
        result = []
        for flower in self.flowers:
            if flower.stem_length == stem_length:
                result.append(flower.__class__.__name__)
        return f'Найдены цветы с длиной стебля в {stem_length} см: {result}'
   
    def find_flower_by_color(self, color):  # Поиск цветка по цвету
        result = []
        for flower in self.flowers:
            if flower.color == color:
                result.append(flower.__class__.__name__)
        return f'Найдены цветы цвета "{color}": {result}'


""" Создание цветков """
rose_1 = Rose(7, 'Red', 50)
tulips_1 = Tulip(3, 'Yellow', 20)
violet_1 = Violet(9, 'Violet', 5)
chamomile_1 = Chamomile(1, 'White', 15)
rose_2 = Rose(8, 'White', 45)

""" Создание букета """
bouqet_1 = Bouqet(rose_1, tulips_1, violet_1, chamomile_1)

""" Вывод всех заданных условий согласно ДР№10 """
print(bouqet_1.get_flowers())
print(bouqet_1.dead_time())
print(bouqet_1.sort_flowers_by_price())
print(bouqet_1.sort_flowers_by_freshness())
print(bouqet_1.sort_flowers_by_length())

print(bouqet_1.__contains__(violet_1))
print(bouqet_1.__contains__(rose_2))

print(bouqet_1.__getitem__(0))
print(bouqet_1.__getitem__(1))
print(bouqet_1.__getitem__(2))

print(bouqet_1.find_flower_by_freshness(9))
print(bouqet_1.find_flower_by_stem_length(15))
print(bouqet_1.find_flower_by_color('White'))

for flowers in bouqet_1:
    print(flowers)
    
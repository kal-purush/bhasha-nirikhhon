# coding: UTF-8

# Задание - 1
# Давайте опишем пару сущностей player и enemy через словарь,
# который будет иметь ключи и значения:
# name - строка полученная от пользователя,
# health - 100,
# damage - 50.
# Поэксперементируйте с значениями урона и жизней по желанию.
# Теперь надо создать функцию attack(person1, persoтn2), аргументы можете указать свои,
# функция в качестве аргумента будет принимать атакующего и атакуемого,
# функция должна получить параметр damage атакующего и отнять это количество
# health от атакуемого. Функция должна сама работать с словарями и изменять их значения.

attaker = {'name': input("Введите имя атакующего: "), 'health': 100, 'damage': 50}
attaked = {'name': input("Введите имя атакуемого: "), 'health': 100, 'damage': 50}

def attack(enemy, player):
    player['health'] = player['health'] - enemy['damage']

attack(attaker, attaked)
print('Здоровье атакуемого после нападения: = ', attaked['health'])


# Задание - 2
# Давайте усложним предыдущее задание, измените сущности, добавив новый параметр - armor = 1.2
# Теперь надо добавить функцию, которая будет вычислять и возвращать полученный урон по формуле damage / armor
# Следовательно у вас должно быть 2 функции, одна наносит урон, вторая вычисляет урон по отношению к броне.

# Сохраните эти сущности, полностью, каждую в свой файл,
# в качестве названия для файла использовать name, расширение .txt
# Напишите функцию, которая будет считывать файл игрока и его врага, получать оттуда данные, и записывать их в словари,
# после чего происходит запуск игровой сессии, где сущностям поочередно наносится урон,
# пока у одного из них health не станет меньше или равен 0.
# После чего на экран должно быть выведено имя победителя, и количество оставшихся единиц здоровья.

attaker = {'name': input("Введите имя атакующего: "), 'health': 100, 'damage': 50, 'armor': 1.2}
attaked = {'name': input("Введите имя атакуемого: "), 'health': 100, 'damage': 50, 'armor': 1.2}

def armor_damage(damage, armor):
    damage = damage / armor

def attack(enemy, player):
    player['health'] = player['health'] - enemy['damage']

file_attaker = attaker['name'] + '.txt'
file_attaked = attaked['name'] + '.txt'

with open(file_attaker, 'w+', encoding='utf-8') as file1:
    for key, value in attaker.items():
        file1.writelines('{} = {}\n'.format(key, value))

with open(file_attaked, 'w+', encoding='utf-8') as file2:
    for key, value in attaked.items():
        file2.writelines('{} = {}\n'.format(key, value))

def find_player(file_name):
    with open(file_name) as file:
        line = file.readlines()

        for line in file:
            result = line.split(',')
            x = result[1]
            y = result[2]
            dict = {}
            key = 0
            value = 1
            # for key in list:
            #     for value in list:
            #         dict['key'] = int(value)
            #         value += 2
            #     key += 2
            print(x, y)


print(find_player(file_attaker))

# запуталась на создании словаря из полученного списка из файла...
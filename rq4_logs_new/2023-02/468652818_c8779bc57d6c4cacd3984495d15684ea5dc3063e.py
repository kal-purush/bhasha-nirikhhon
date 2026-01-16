import random

role = {'1': 'Воин', '2': 'Лучник', '3': 'Маг'}


classes = {

    'Воин': {
        'здоровье': 100,
        'атака': 30,
        'защита': 25,
        'навыки': {
            'щит': 10
        }
    },

    'Лучник': {
        'здоровье': 50,
        'атака': 40,
        'защита': 20,
        'навыки': {
            'убежать': 10
        }
    },

    'Маг': {
        'здоровье': 30,
        'атака': 50,
        'защита': 15,
        'навыки': {
            'магический щит': 10,
            'лечение': 10
        }
    }
}


def is_valid(text: str, is_role: bool = False) -> bool:
    if len(text) == 0:
        print('Ошибка ввода. Вы ввели пустую строку.')
        return False
    elif text not in '123' and is_role == True:
        print('Ошибка ввода. Вы ввели не правильное значение. Введите числа от 1 до 3.')
        return False
    else:
        return True




# Инициализация персонажа (создание)

def init_person(name: str, is_enemy: bool = False):
    if is_enemy:
        person = {'класс': role[random.choice(list(role.keys()))]}
    else:
        while True:
            choice = input('Введите класс: 1-Воин, 2-Лучник, 3-Маг\n')
            person = {'класс': role[choice]}

    person.update({'характеристики': classes[person['класс']]})
    person.update({'имя': name})

    print(f"{person['имя']} - {person['класс']}, имеет характеристики: {person['характеристики']}")
    return person









player = init_person(input('Введите имя игрока: '))
enemy = init_person('Враг', True)
"""
    == Лото ==
    Правила игры в лото.
    Игра ведется с помощью специальных карточек, на которых отмечены числа,
    и фишек (бочонков) с цифрами.
    Количество бочонков — 90 штук (с цифрами от 1 до 90).
    Каждая карточка содержит 3 строки по 9 клеток. В каждой строке по 5 случайных цифр,
    расположенных по возрастанию. Все цифры в карточке уникальны. Пример карточки:
    --------------------------
        9 43 62          74 90
     2    27    75 78    82
       41 56 63     76      86
    --------------------------
    В игре 2 игрока: пользователь и компьютер. Каждому в начале выдается
    случайная карточка.
    Каждый ход выбирается один случайный бочонок и выводится на экран.
    Также выводятся карточка игрока и карточка компьютера.
    Пользователю предлагается зачеркнуть цифру на карточке или продолжить.
    Если игрок выбрал "зачеркнуть":
        Если цифра есть на карточке - она зачеркивается и игра продолжается.
        Если цифры на карточке нет - игрок проигрывает и игра завершается.
    Если игрок выбрал "продолжить":
        Если цифра есть на карточке - игрок проигрывает и игра завершается.
        Если цифры на карточке нет - игра продолжается.

    Побеждает тот, кто первый закроет все числа на своей карточке.
    Пример одного хода:
    Новый бочонок: 70 (осталось 76)
    ------ Ваша карточка -----
     6  7          49    57 58
       14 26     -    78    85
    23 33    38    48    71
    --------------------------
    -- Карточка компьютера ---
     7 11     - 14    87
          16 49    55 77    88
       15 20     -       76  -
    --------------------------
    Зачеркнуть цифру? (y/n)
    Подсказка: каждый следующий случайный бочонок из мешка удобно получать
    с помощью функции-генератора.
    Подсказка: для работы с псевдослучайными числами удобно использовать
    модуль random: http://docs.python.org/3/library/random.html
"""

import random
import sys

ballsleft = 90
p1 = 15
p2 = 15
balls = random.sample(range(90), 90)
game_set = random.sample(range(90), 30)
p1_set = random.sample(game_set, 15)
p2_set = [e for e in game_set if not e in p1_set]
p1_field = [p1_set[:5], p1_set[5:10], p1_set[10:]]
p2_field = [p2_set[:5], p2_set[5:10], p2_set[10:]]

for p1line in p1_field:
    p1line.sort()
    p1line.insert(random.randint(0, 4), ' ')
    p1line.insert(random.randint(0, 5), ' ')
    p1line.insert(random.randint(0, 6), ' ')
    p1line.insert(random.randint(0, 7), ' ')

for p2line in p2_field:
    p2line.sort()
    p2line.insert(random.randint(0, 4), ' ')
    p2line.insert(random.randint(0, 5), ' ')
    p2line.insert(random.randint(0, 6), ' ')
    p2line.insert(random.randint(0, 7), ' ')


def field(p):
    if p == 0:
        print('{:-^26}'.format(' Ваша карточка '))
        for line1 in p1_field:
            for n1 in line1:
                print('{0:>2}'.format(n1), end=' ')
            print()
        print('{:-^26}\n'.format('-'))
    if p == 1:
        print('{:-^26}'.format(' Карточка компьютера '))
        for line2 in p2_field:
            for n2 in line2:
                print('{0:>2}'.format(n2), end=' ')
            print()
        print('{:-^26}\n'.format('-'))


def you_move():
    a = input('Зачеркнуть цифру? (y/n): ')
    if a == 'y':
        if ball in p1_set:
            for l in p1_field:
                try:
                    l.insert(l.index(ball), '-')
                    l.pop(l.index(ball))
                except ValueError:
                    continue
            print('Ок')
            return 1
        else:
            print('Игра окончена')
            sys.exit()
    if a == 'n':
        if ball in p1_set:
            print('Игра окончена')
            sys.exit()
        else:
            print('Ок')


def com_move():
    if ball in p2_set:
        for i in p2_field:
            try:
                i.insert(i.index(ball), '-')
                i.pop(i.index(ball))
            except ValueError:
                continue
        return 1


for ball in balls:
    ballsleft -= 1
    print('\nНовый бочонок: {} (осталось: {})\n'.format(ball, ballsleft))
    field(0)
    field(1)
    if you_move() == 1:
        p1 -= 1
    if com_move() == 1:
        p2 -= 1
    if p1 == 0:
        print('Вы выиграли')
        sys.exit()
    if p2 == 0:
        print('Вы проиграли')
        sys.exit()
    if ballsleft == 0:
        print('Игра окончена')
        sys.exit()
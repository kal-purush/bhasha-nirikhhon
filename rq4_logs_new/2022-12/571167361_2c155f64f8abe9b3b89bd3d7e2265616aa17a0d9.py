print('Движение по координатам')
x = 0
y = 0
print('Начальная позиция: (',x,';',y,')')
ch = True
while ch:
    a = input('В каком направлении хотите переместиться?\nНаправление: ')
    n = int(input('На какое расстояние хотите перемститься?\nРасстояние: '))
    if a == 'U' or a == 'u':
        y+=n
    elif a == 'D' or a == 'd':
        y = y - n
    elif a == 'R' or a == 'r':
        x+=n
    elif a == 'L' or a == 'l':
        x= x - n
    print('Переместились в: (',x,';',y,')')
    ex = input('\nХотите поменять свою позицию?\n Y - Да\n Любой другой символ - Exit\n')
    if ex == 'Y' or ex == 'y':
        continue
    else:
        print('Пока-пока!')
    if ex == "Y":
        ch = True
    else:
        ch = False
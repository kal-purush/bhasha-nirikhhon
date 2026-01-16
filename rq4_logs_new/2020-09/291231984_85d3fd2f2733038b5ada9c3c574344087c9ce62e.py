# Реализовать скрипт, в котором должна быть предусмотрена функция расчета заработной платы сотрудника.
# В расчете необходимо использовать формулу: (выработка в часах*ставка в час) + премия.
# Для выполнения расчета для конкретных значений необходимо запускать скрипт с параметрами.

from sys import argv

cnt_params = len(argv) - 1

hours = 0
price = 0
prize = 0

is_err = False

if cnt_params != 3:
    print("д.б. указано 3 параметра, а указано ", cnt_params)
    is_err = True
else:
    try:
        hours = float(argv[1])
    except ValueError:
        print("неверное значение параметра 'выработка в часах': ", argv[1])
        is_err = True

    try:
        price = float(argv[2])
    except ValueError:
        print("неверное значение параметра 'ставка в час': ", argv[2])
        is_err = True

    try:
        prize = float(argv[3])
    except ValueError:
        print("неверное значение параметра 'премия': ", argv[3])
        is_err = True

if not is_err:
    print("Заработная плата: ", hours * price + prize)


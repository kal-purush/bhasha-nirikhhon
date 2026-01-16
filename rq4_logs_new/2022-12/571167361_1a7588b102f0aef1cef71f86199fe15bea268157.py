d = {
    '900': 'Кирка',
    '800': 'Тесак', 
    '700': 'Бронежилет'
}
ch = True
while ch:
    wght = 0
    maxwght = 3000
    len_d = len(d)
    print(d)
    d_keys = list(map(int, d.keys()))
    l_d_keys = len(d_keys)
    for i in range(l_d_keys):
        wght+= d_keys[i]
    ex = input(' 1. Добавить вещь\n 2. Удалить вещь\n 3. Выход')
    print('Свободного места:',int(maxwght-wght), 'ед.')
    if ex == "1":
        w = int(input('Какой вес у вещи?\n'))
        if w <=int(maxwght-wght):
            n = input('Как она называется?\n')
            d[w] = n
        if w >=int(maxwght-wght):
            print('Слишком большой вес!')
    if ex == "2":
        c = input('Введите вес вещи:')
        del d[c]
        print(d)
    if ex == "3":
        ch = False
    else:
        ch = True
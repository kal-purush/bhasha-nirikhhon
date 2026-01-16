import os

''' Столкнулся с ошибкой: os.remove('dir_{}'.format(i+1))
                          PermissionError: [WinError 5] Отказано в доступе: 'dir_1'
    Не хватает прав доступа. Видимо нужно запустить интерпретатор с правами администратора, 
    как это сделать - не разобрался
'''
for i in range(9):
    if not os.path.exists('dir_{}'.format(i+1)):
        os.mkdir('dir_{}'.format(i+1))
    else:
        print('Такая директория уже есть')
print(os.listdir())

for i in range(9):
    os.remove('dir_{}'.format(i+1))
print(os.listdir())


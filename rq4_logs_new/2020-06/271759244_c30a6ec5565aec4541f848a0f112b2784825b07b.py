print('task 1')
a = 43
my_str = 'example'
my_tuple = (22, 'slot1')
b = set('majic')
c = {'key_1':'ex1', 'key_2':321}

my_list = [a, my_str, my_tuple, b, c]
print(my_list)

for itm in my_list:
    print(itm, type(itm))


print('task 2')
user_answer = input('Введите список через запятую ')
user_list = user_answer.split(',')

idx = 0
while idx < len(user_list[:-1]):
    user_list[idx], user_list[idx + 1] = user_list[idx + 1], user_list[idx]
    idx += 2
print(user_list)

print('task 3')
user_answer1 = int(input("Введите месяц в виде целого числа от 1 до 12 "))
seas_list = ['win', 'spr', 'sum', 'aut']
seas_dict = {1 : 'win', 2 : 'spr', 3 : 'sum', 4 : 'aut'}

if user_answer1 == 1 or user_answer1 == 12 or user_answer1 == 2:
        print(seas_dict.get(1))
        print(seas_list[0])
elif user_answer1 == 3 or user_answer1 == 4 or user_answer1 ==5:
    print(seas_dict.get(2))
    print(seas_list[1])
elif user_answer1 == 6 or user_answer1 == 7 or user_answer1 == 8:
    print(seas_dict.get(3))
    print(seas_list[2])

elif user_answer1 == 9 or user_answer1 == 10 or user_answer1 == 11:
    print(seas_dict.get(4))
    print(seas_list[3])
else:
        print("Вы ввели неверное значение")


print('task 4')
user_str = input('Введите несколько слов через пробел ')
user_list1 = user_str.split(' ')
print(user_list1)
for i in user_list1:

    if len(i) <= 10:
        print(i)

    elif len(i) > 10:
        print(i[:10])

print('task 5')
my_list1 = [4, 8, 2, 2, 6]
user_number = input('Введите новый элемент рейтинга ')
user_number = int(user_number)
my_list1.append(user_number)
my_list1.sort()
my_list1.reverse()
print(my_list1)

print('task 6')
products = int(input("Введите количество товара "))
n = 1
my_dict = []
my_list = []
my_analys = {}
while n <= products:
    my_dict = dict({'название': input("введите название "), 'цена': input("Введите цену "),
                    'количество': input('Введите количество '), 'eд': input("Введите единицу измерения ")})
    my_list.append((n, my_dict))
    n += 1
    my_analys = dict(
        {'название': my_dict.get('название'), 'цена': my_dict.get('цена'), 'количество': my_dict.get('количество'),
         'ед': my_dict.get('ед')})
print(my_list)
print(my_analys)




























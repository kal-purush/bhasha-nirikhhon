my_f = open("names.txt", encoding="utf-8")
content = my_f.readlines()
new_list = []
for i in range(len(content)):
    n = content[i].split(' ')
    new_list.extend(n)
for i in range(1, len(new_list), 2):
    new_list.insert(i, float(new_list[i][:-1]))
    new_list.pop(i + 1)
my_dict = {new_list[i]: new_list[i + 1] for i in range(0, len(new_list), 2)}
print('Сотрудники с окладом менее 20000: ')
sum_averedg = 0
for k, v in my_dict.items():
    sum_averedg += v
    if v <= 20000:
        print(k)

print('Средний доход составляет:', sum_averedg / len(my_dict))

my_f.close()
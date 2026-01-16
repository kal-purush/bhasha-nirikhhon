# Задание-1:
# Напишите скрипт, заполняющий указанный файл (самостоятельно задайте имя файла)
# произвольными целыми цифрами, в результате в файле должно быть
# 2500-значное произвольное число.
# Найдите и выведите самую длинную последовательность одинаковых цифр
# в вышезаполненном файле.


# СНАЧАЛА СОЗДАЕМ СПИСОК ДО 2500-ЗНАЧНОГО ЧИСЛА
# ИЩЕМ  САМУЮ ДЛИННУЮ ПОСЛЕДОВАТЕЛЬНОСТЬ ОДИНАКОВЫХ ЧИСЕЛ
# ПОТОМ РЕШАЕМ ВОПРОС С ФАЙЛАМИ
import random

# A = [0, 0, 0, 0, 0, 0]
A = []
for i in range(2500):
    A.append(random.randint(0, 9))
str_A = "".join([str(x) for x in A])
print(str_A)
print(len(str_A))
with open('file.txt', 'w') as f:
    f.write(str_A)
if len(str_A) > 1:
    count = str(str_A[0])
    top = str(str_A[0])
    for j in range(len(str_A) - 1):
        if str_A[j] == str_A[j + 1]:
            count += str_A[j]
        else:
            if len(top) < len(count):
                top = count
            count = str_A[j+1]
    print(top, len(top)) if len(top) > len(count) else print(count, len(count))
else:
    print(str_A, len(str_A))

f.close()

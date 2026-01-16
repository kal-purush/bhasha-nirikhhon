# coding: utf-8


# EASY
# задача 1

a = "Добро пожаловать, "
b = 1
d = input("Введите Ваше имя: ")

print(a, d, b)

# задача 2

c = int(input("Введите любое число: "))

print(c + 2)

# задача 3

r = int(input("Сколько Вам лет? "))

if r >= 18:
    print("Доступ разрешен!")
else:
    print("Извините, пользование данным ресурсом только с 18 лет!")

# NORMAL
# задача 1

a = int(input("Введите число от нуля до десяти: "))

while a < 0 or a >10:
   if a < 0:
       print("Число меньше нуля!")
       a = int(input("Введите число от нуля до десяти: "))
   elif a > 10:
       print("Число больше десяти!")
       a = int(input("Введите число от нуля до десяти: "))
else:
    print(a ** 2)

# задача 2

b = int(input("Введите число b: "))
c = int(input("Введите число c: "))

b = b + c
c = b - c
b = b - c

print("Число b = ", b)
print("Число с = ", c)

# HARD

name = input("Введите имя: ")
surname = input("Введите фамилию: ")
age = int(input("Введите свой возраст: "))
weight = int(input("Введите свой вес: "))

if age <= 30 and 50 < weight < 120:
    print("Поздравляем! ", name, surname, ", Вы в отличной форме!")
elif 40 > age > 30 and weight > 120:
    print("Может, в спортзал?")
elif 40 > age > 30 and weight < 50:
    print("Может, в спортзал?")
elif age > 40 and weight <50:
    print(name, surname, ", пора к врачу!")
elif age > 40 and weight > 120:
    print(name, surname, ", пора к врачу!")
else:
    print("Неизвестная категория здоровых людей!")

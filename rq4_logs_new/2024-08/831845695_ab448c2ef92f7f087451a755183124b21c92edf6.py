# Task 1
'''Порахувати кількість унікальних символів в строці.
Якщо їх більше 10 - вивести в консоль True, інакше - False.
Строку отримати за допомогою функції input()'''

proposed_string: str = input("Provide your string here, please: ")
unique_counter: int = sum([1 for char in proposed_string if proposed_string.count(char) == 1])

if unique_counter > 10:
    print(True)
else:
    print(False)

# Task 2

is_correct_str: bool = False

while not is_correct_str:
    provided_word: str = input("Provide your word, please: ")

    if provided_word.find("h") == -1 or provided_word.find("H") != -1:
        is_correct_str = True

# Task 3

lst1 = ['1', '2', 3, True, 'False', 5, '6', 7, 8, 'Python', 9, 0, 'Lorem Ipsum']
desired_list: list[str] = [item for item in lst1 if isinstance(item, str)]
print(desired_list)

# Task 4

"""Є ліст з числами, порахуйте сумму усіх ПАРНИХ чисел в цьому лісті"""

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
numbers_summ = sum(num for num in numbers if num % 2 == 0)
print("Сума всіх парних чисел: ", numbers_summ)
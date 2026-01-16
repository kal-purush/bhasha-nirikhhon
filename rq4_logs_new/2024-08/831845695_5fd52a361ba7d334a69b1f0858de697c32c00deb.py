# task 1
""" Задача - надрукувати табличку множення на задане число, але
лише до максимального значення для добутку - 25.
Код майже готовий, треба знайти помилки та випраавити\доповнити.
"""
def multiplication_table(number):
    # Initialize the appropriate variable
    multiplier = 1

    # Complete the while loop condition.
    while True:
        result = number * multiplier
        # десь тут помила, а може не одна
        if result > 25:
            break
            # Enter the action to take if the result is greater than 25
        print(str(number) + "x" + str(multiplier) + "=" + str(result))

        # Increment the appropriate variable
        multiplier += 1

multiplication_table(3)
# Should print:
# 3x1=3
# 3x2=6
# 3x3=9
# 3x4=12
# 3x5=15

# Task 1.1 (самостоятельная работа для практики и закрепления материала)
"""Написать программу, которая генерирует таблицу сложения для заданного числа, но только до максимального значения суммы - 50.
Пример: Если задано число 8, программа должна выводить:

8 + 1 = 9
8 + 2 = 10
8 + 3 = 11
...
Программа должна остановиться, как только сумма станет больше 50."""

def addition_table(number):
    summand = 1

    while True:
        result = number + summand
        if result > 50:
            break
        print(str(number) + "+" + (str(summand) + "=" + str(result)))
        summand += 1
addition_table(8)



# Task 1.2 (самостоятельная работа для практики и закрепления материала)
"""Написать программу, которая генерирует таблицу вычитания для заданного числа, но только до минимального значения разности - 0.
Пример: Если задано число 15, программа должна выводить:

15 - 1 = 14
15 - 2 = 13
15 - 3 = 12
...
Программа должна остановиться, как только разность станет равна 0 или меньше."""

def subtraction_table(number):
    difference = 1

    while True:
        result = number - difference
        if result <= 0:
            break
        print(str(number) + "-" + str(difference) + "=" + str(result))
        difference += 1
subtraction_table(15)

# task 2
"""  Написати функцію, яка обчислює суму двох чисел.
"""
def sum_of_two_numbers(a, b):
    return a+b
result = sum_of_two_numbers(2, 3)
print(result)


# task 3
"""  Написати функцію, яка розрахує середнє арифметичне списку чисел.
"""
def average_of_list(numbers):
    if not numbers:
        return None

    total_summ = sum(numbers)
    count = len(numbers)
    return total_summ / count

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
result = average_of_list(numbers)
print(result)

# task 4
"""  Написати функцію, яка приймає рядок та повертає його у зворотному порядку.
"""
def reverse_string(x):
    return x[::-1]
text = "Hello, World"
result = reverse_string(text)
print(result)

# task 5
"""  Написати функцію, яка приймає список слів та повертає найдовше слово у списку.
"""
def longest_word(words):
    if not words:
        return None
    longest = max(words, key = len)
    return longest

words_list = ["Apple", "Samsung", "Xiaomi", "LG", "OnePlus"]
result = longest_word(words_list)
print(result)

# task 6
"""  Написати функцію, яка приймає два рядки та повертає індекс першого входження другого рядка
у перший рядок, якщо другий рядок є підрядком першого рядка, та -1, якщо другий рядок
не є підрядком першого рядка."""
def find_substring(str1, str2):

    return -1

str1 = "Hello, world!"
str2 = "world"
print(find_substring(str1, str2)) # поверне 7

str1 = "The quick brown fox jumps over the lazy dog"
str2 = "cat"
print(find_substring(str1, str2)) # поверне -1

# task 7
# task 8
# task 9
# task 10
"""  Оберіть будь-які 4 таски з попередніх домашніх робіт та
перетворіть їх у 4 функції, що отримують значення та повертають результат.
Обоязково документуйте функції та дайте зрозумілі імена змінним.
"""
# Tasks from Homework 06

# Task 1
def has_more_than_ten_unique_chars(input_string: str) -> bool:
    """Подсчитываем количество уникальных символов в строке.
    Если количество уникальных символов больше 10, возвращаем True, если другое значение - False."""

    unique_counter = sum([1 for char in input_string if input_string.count(char) == 1])
    return unique_counter > 10

provided_string = input("Provide your string here, please: ")
print(has_more_than_ten_unique_chars(provided_string))

#Task 2

def is_valid_word(input_word: str) -> bool:
    """Проверим, содержит ли строка 'h' и не содержит ли 'H'. """
    return input_word.find("h") != -1 and input_word.find("H") == -1

is_correct_str = False
while not is_correct_str:
    provided_word = input("Provide your word, please: ")
    is_correct_str = is_valid_word(provided_word)

# Task 3
def filter_strings_from_list(input_list: list) -> list[str]:
    """Фильтрует строки из списка."""
    return [item for item in input_list if isinstance(item, str)]

lst1 = ['1', '2', 3, True, 'False', 5, '6', 7, 8, 'Python', 9, 0, 'Lorem Ipsum']
print(filter_strings_from_list(lst1))

#Task 4
def sum_of_even_numbers(numbers: list[int]) -> int:
    """
    Вычисляет сумму всех четных чисел в списке."""
    return sum(num for num in numbers if num % 2 == 0)

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print("Сума всіх парних чисел:", sum_of_even_numbers(numbers))
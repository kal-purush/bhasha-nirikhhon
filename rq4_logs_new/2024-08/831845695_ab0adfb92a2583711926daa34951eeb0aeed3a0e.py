import re

"""Для кожного елементу списку виведіть суму всіх чисел (створіть нову функцію для цього).
Якщо є символи, що не є числами (”qwerty1,2,3” у прикладі), вам потрібно зловити вийняток і вивести “Не можу це зробити!”
Використовуйте блок try\except, щоб уникнути інших символів, окрім чисел у списку.
Для цього прикладу правильний вивід буде - 10, 60, “Не можу це зробити”"""

string_array = [
    "1,2,3,4",
    "1,2,3,4,50",
    "qwerty1,2,3"]

def sum_of_numbers_in_string(stroka):
    try:
        # Перевіряємо, чи є рядок лише числами та комами
        if re.search(r'[^0-9,]', stroka):
            raise ValueError("Рядок містить недопустимі символи")

        numbers = re.findall(r'\d+', stroka)
        total_sum = sum(int(num) for num in numbers)
        return total_sum
    except ValueError:
        return "Не можу це зробити!"

for string in string_array:
    result = sum_of_numbers_in_string(string)
    print(result)

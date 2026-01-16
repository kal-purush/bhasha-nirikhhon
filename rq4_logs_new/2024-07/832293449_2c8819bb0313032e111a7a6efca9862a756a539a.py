import re
from typing import Callable

def generator_numbers(text: str):
    pattern = r"(\s\d+[.]\d+\s)"
    values = re.findall(pattern, text)
    for value in values:
        yield value

def sum_profit(text: str, func: Callable):
    gen = func(text)
    sum = 0
    for value in gen:
        sum = sum + float(value)
    return sum

entered_text = "Загальний дохід працівника складається з декількох частин: 1000.01 як основний дохід, доповнений додатковими надходженнями 27.45 і 324.00 доларів."

total_income = sum_profit(entered_text, generator_numbers)
print(f"Загальний дохід: {total_income}")

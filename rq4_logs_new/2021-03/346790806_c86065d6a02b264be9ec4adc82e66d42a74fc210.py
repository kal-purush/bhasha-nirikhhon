import sys

def fibonacci(num):
    if not isinstance(num, int):
        return None
    if num < 0:
        return None
    if num <= 1:
        return num
    return fibonacci(num-1) + fibonacci(num-2)


def factorial(num):
    if not isinstance(num, int):
        return None
    if num < 0:
        return None
    if num <= 1:
        return 1
    return num * factorial(num - 1)
'''This program is to find out the product of the prime factors of a given number'''
from pyproblems.utility import is_int
def product_prime(num1):
    '''This function returns the product of the numbers which are prime factors
    of a given number'''
    if not is_int(num1):
        raise TypeError(f'unsupported format, {type(num1)} is in the place of"integer"')
    if num1 <= 0:
        raise ValueError("pass an integer greater than 0")
    product = 1
    for i in range(2, num1+1):
        if num1 % i == 0:
            is_prime = 1
            for j in range(2, int(i/2 + 1)):
                if i % j == 0:
                    is_prime = 0
                    break
            if is_prime:
                product = product * i
    return product
    
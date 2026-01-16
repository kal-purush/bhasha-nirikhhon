'''Given a number, returns the prime number that comes first when we count backward'''
from pyproblems.prime import is_prime
from pyproblems.utility import is_int
def prime_before(number):
    '''The program will return the prime number that comes first
    when we count from backwards of the given number
    Note: The number passed should be greater than 2'''
    if not is_int(number):
        raise TypeError("Unsupported format")
    if number <= 2:
        raise ValueError('Enter a value greater that 2')
    primes_befor_n = []
    for i in range(2, number):
        if is_prime(i):
            primes_befor_n.append(i)
    return max(primes_befor_n)
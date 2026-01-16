'''This program is to find out the given number is an abundant number or not'''
def is_abundant(number):
    '''The above function will return True if the given number is an abundant number or not.
    Abundant Number: if the sum of the divisors(ecluding the given number) of given number,
    exceeds the given number then it is said to be abundant
    Note: all prime numbers are not abundant.'''

    if not isinstance(number, int) or isinstance(number, bool):
        raise TypeError("unsupported format, Enter an Integer")

    sum_divisors = 0
    for i in range(1, number):
        if number%i == 0:
            sum_divisors += i

    return bool(sum_divisors > number)
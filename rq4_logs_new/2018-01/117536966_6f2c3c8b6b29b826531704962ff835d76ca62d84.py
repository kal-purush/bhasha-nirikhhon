"""
Functions for representing numbers in plain English.
"""

import argparse
import collections
import functools
import random
import re
import string


DIGIT_NAMES = {
    0: 'zero',
    1: 'one',
    2: 'two',
    3: 'three',
    4: 'four',
    5: 'five',
    6: 'six',
    7: 'seven',
    8: 'eight',
    9: 'nine',
}

NUMBER_NAMES = {
    10: 'ten',
    11: 'eleven',
    12: 'twelve',
    13: 'thirteen',
    14: 'fourteen',
    15: 'fifteen',
    16: 'sixteen',
    17: 'seventeen',
    18: 'eighteen',
    19: 'nineteen',
    20: 'twenty',
    30: 'thirty',
    40: 'forty',
    50: 'fifty',
    60: 'sixty',
    70: 'seventy',
    80: 'eighty',
    90: 'ninety',
}

UNIQUE_NUMBER_NAMES = {**DIGIT_NAMES, **NUMBER_NAMES}

ILLIONS = (
    'm',
    'b',
    'tr',
    'quadr',
    'quint',
    'sext',
    'sept',
    'oct',
    'non',
    'dec',
    'undec',
    'duodec',
    'tredec',
    'quattuordec',
    'quindec',
    'sexdec',
    'septendec',
    'octodec',
    'novemdec',
    'vigint',
    'unvigint',
    'duovigint',
    'trevigint',
    'quattuorvigint',
    'quinvigint',
    'sexvigint',
    'septenvigint',
    'octovigint',
    'novemvigint',
    'trigint',
    'untrigint',
    'duotrigint',
)


# Crudely calculated maximum representable number.
MAX = 10**((len(ILLIONS) + 2)*3) - 1


def last_to_first_digit(number: int, *, base: int=10):
    """ Yield each digit of an integer, from least significant to most.
    """
    while number:
        yield number % base
        number //= base


def integer_string(number, *, use_and=True) -> str:
    """ Return an integers value in plain English.
    """
    if not number:
        return DIGIT_NAMES[number]

    sign_prefix = 'minus ' if number < 0 else ''
    number = abs(number)

    thousand_chunks = collections.deque()

    chunks = last_to_first_digit(number, base=1000)

    for chunk_index, chunk in enumerate(chunks):

        # Group is zero, skip.
        if not chunk:
            continue

        words = []
        add_word = words.append

        hundreds = chunk // 100
        tens = chunk % 100

        if tens in UNIQUE_NUMBER_NAMES:
            tens_name = UNIQUE_NUMBER_NAMES[tens]
        else:
            digit = tens % 10
            tens -= digit
            tens_name = f'{NUMBER_NAMES[tens]}-{DIGIT_NAMES[digit]}'

        if hundreds:
            add_word(DIGIT_NAMES[hundreds])
            add_word('hundred')
            if use_and and tens:
                add_word('and')
        if tens:
            add_word(tens_name)

        if chunk_index == 1:
            add_word('thousand')
        elif chunk_index > 1:
            add_word(ILLIONS[chunk_index - 2] + 'illion')

        thousand_chunks.appendleft(' '.join(words))

    return sign_prefix + ', '.join(thousand_chunks)


INT_PATTERN = r'0|([1-9]\d+(_\d+)*)'

INT_REGEX = re.compile(INT_PATTERN)
FLOAT_REGEX = re.compile(INT_PATTERN + r'\.' + INT_PATTERN)


def number_string(number) -> str:
    """ Return a numbers value in plain English.
    """
    # String handling.
    if isinstance(number, (str)):
        if INT_REGEX.fullmatch(number):
            return integer_string(int(number))

    # Integer handling.
    if isinstance(number, int):
        return integer_string(number)

    # Float handling.
    try:
        integer, points = str(number).split('.')
    except ValueError:
        raise ValueError(f'Cannot parse number as float: {number}')

    integer_part = integer_string(int(integer))
    points_part = ' '.join(DIGIT_NAMES[int(digit)] for digit in points)

    return f'{integer_part} point {points_part}'


def random_integer_string(max_digits):
    """ Return a string representing a number with a random number
    (between 1 and max_digits) of digits.
    """
    first = str(random.randrange(1, 10))

    integer_count = random.randrange(0, max_digits - 1)
    tail = ''.join(random.choices(string.digits, k=integer_count))

    return first + tail


def random_number_string(max_integer_digits=100, max_decimal_points=10):
    """ Return a random length number string, equal chance of a float or
    an integer.
    """
    integer = random_integer_string(max_integer_digits)

    if random.random() < 0.5:
        return integer
    return integer + '.' + random_integer_string(max_decimal_points)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('number', nargs='?')
    parser.add_argument('--max', action='store_true')

    args = parser.parse_args()
    number = args.number

    if args.max:
        number = MAX
    elif number is None:
        number = random_number_string()

    print(f'{number}: {number_string(number)}')

if __name__ == '__main__':
    main()
#!/usr/bin/env python3

def my_grep(target, my_string):
    lines = my_string.split('\n')
    result = [line for line in lines if target in line]
    return '\n'.join(result)

def count_vowels(my_string):
    vowels = 'aeiouAEIOU'
    return sum(1 for char in my_string if char in vowels)

def reverse_string(my_string):
    return my_string[::-1]
def reverse_string(word):
    return ''.join(reversed(word))

def test_reverse_string():
    input_str = "TripleTen"
    reversed_str = reverse_string(input_str)
    assert reversed_str == "neTelpirT"
    print("Teste Aprovado! " + input_str + " invertido é " + reversed_str)

def is_palindrome(word):
    reversed_str = ''.join(reversed(word))
    return word == reversed_str

def test_is_palindrome():
    input_str = "osso"
    result = is_palindrome(input_str)
    assert result == True
    print("Teste Aprovado! " + input_str + " é um palíndromo.")

import math

def compute_factorial(number):
    return math.factorial(number)

def test_compute_factorial():
    input_number = 5
    result = compute_factorial(input_number)
    assert result == 120
    print("Teste aprovado! O fatorial de " + str(input_number) + " é " + str(result))
def get_string(number):
    result = number

    if number % 3 == 0:
        result = 'Python'
    elif number % 5 == 0:
        result = 'Diario'

    return result
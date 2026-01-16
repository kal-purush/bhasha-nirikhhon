def is_even_odd_number(num):
    if not isinstance(num, int):
        return None

    return (num & 1) == 0  # Even number
    # return (num & 1) == 1  # Odd number


print(is_even_odd_number(2))
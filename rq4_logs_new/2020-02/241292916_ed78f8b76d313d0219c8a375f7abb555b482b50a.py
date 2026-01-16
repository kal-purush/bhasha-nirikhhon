import random

def random_pair(number_of_items):
    left = random.randint(1, number_of_items)
    right = random.randint(1, number_of_items)
    while left == right:
        right = random.randint(1, number_of_items)
    pair = (left, right)
    return pair

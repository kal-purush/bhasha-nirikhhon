import math


def is_enough(n, current_n, width, height):
    square_width = current_n * width
    square_height = math.ceil(n / current_n) * height
    return square_width >= square_height


def find_min_side(n, width, height):
    left_border = 1
    right_border = n
    min_square_side = n * width
    while left_border <= right_border:
        current_position = (left_border + right_border) // 2
        if is_enough(n, current_position, width, height):
            min_square_side = current_position * width
            right_border = current_position - 1
        else:
            left_border = current_position + 1
    return min_square_side


def get_min_square_size(n, width, height):
    min_side_by_width = find_min_side(n, width, height)
    min_side_by_height = find_min_side(n, height, width)
    return min(min_side_by_width, min_side_by_height)

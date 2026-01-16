from util import iohandler
from copy import deepcopy

# --- solution ---

directions = {
    'dl': [-1, -1],
    'l': [-1, 0],
    'ul': [-1, 1],
    'd': [0, -1],
    'u': [0, 1],
    'dr': [1, -1],
    'r': [1, 0],
    'ur': [1, 1]
}


def get_occupied_seats_by_view(input_file):
    waiting_area = [list(row.strip()) for row in input_file]
    mutator = deepcopy(waiting_area)  # deepcopy needed if mutable inner object/collections present
    width, height = len(waiting_area[0]), len(waiting_area)
    stable, is_odd_round = False, True

    while not stable:
        for row_index in range(height):
            for position_index in range(width):
                row, pos = int(row_index), int(position_index)
                marker = waiting_area[row][pos]
                if marker == '.':
                    continue

                adj_population = count_occupied_adjacency(waiting_area, row, pos, width, height)
                if is_odd_round:
                    if adj_population == 0:
                        mutator[row][pos] = '#'
                else:
                    if adj_population >= 5:
                        mutator[row][pos] = 'L'

        is_odd_round = not is_odd_round
        if waiting_area == mutator:
            stable = True
        else:
            waiting_area = deepcopy(mutator)

    return count_in_collection(waiting_area, '#')


def count_occupied_adjacency(waiting_area, row_index, position_index, width, height):
    occupied_count = 0
    for d in directions.values():
        dimension_seat_found = False
        distance = 1

        while not dimension_seat_found:
            row = row_index + d[1] * distance
            pos = position_index + d[0] * distance

            if pos < 0 or pos >= width or row < 0 or row >= height:
                dimension_seat_found = True  # not really :)
                continue

            marker = waiting_area[row][pos]
            if marker != '.':
                dimension_seat_found = True
                if marker == '#':
                    occupied_count += 1

            distance += 1

    return occupied_count


def count_in_collection(coll, ch):
    return sum([row.count(ch) for row in coll])


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(get_occupied_seats_by_view(iohandler.begin(__file__))))
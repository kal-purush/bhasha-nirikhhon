from util import iohandler

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


def get_occupied_seats(input_file):
    waiting_area = [list(row.strip()) for row in input_file]
    mutator = waiting_area.copy()
    width = len(waiting_area[0])
    height = len(waiting_area)

    for row_index in range(len(waiting_area)):
        for position_index in range(len(waiting_area[row_index])):
            if waiting_area[int(row_index)][int(position_index)] != '.' and \
                    count_occupied_adjacency(waiting_area, int(row_index), int(position_index), width, height) == 0:
                mutator[int(row_index)][int(position_index)] = '#'

    waiting_area = mutator.copy()

    print(mutator)

    return ''


def count_occupied_adjacency(waiting_area, row_index, position_index, width, height):
    occupied_count = 0
    for d in directions.values():
        if width < position_index + d[0] < 0:
            continue
        elif height < row_index + d[1] < 0:
            continue

        if waiting_area[row_index][position_index] == '#':
            occupied_count += 1

    return occupied_count


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(get_occupied_seats(iohandler.begin(__file__))))
from util import iohandler

# --- solution ---


def init(input_file):
    return [str(row.strip()) for row in input_file]


def own_seat_id(seat_list):
    occupied = set([calculate_binary_seat_partition(seat_code) for seat_code in seat_list])
    return set(range(min(occupied), max(occupied))).difference(occupied).pop()


def calculate_binary_seat_partition(seat_code):
    rows, seats = [0, 127], [0, 7]
    [partition(rows, seat_code[index]) for index in range(7)]
    [partition(seats, seat_code[index]) for index in range(7, 10)]

    return rows[0] * 8 + seats[0]


def partition(collection, instruction):  # parameters pass by reference
    median = int((collection[1] - collection[0]) / 2) + 1
    if instruction in ['F', 'L']:
        collection[1] -= median
    elif instruction in ['B', 'R']:
        collection[0] += median


# --- solution ---

if __name__ == '__main__':
    input_list = init(iohandler.begin(__file__))
    iohandler.end(str(own_seat_id(input_list)))
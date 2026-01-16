from util import iohandler

# --- solution ---


def sum_of_contiguous_range_edges(input_file, preamble_size):
    data_list = [int(row.strip()) for row in input_file]
    invalid_record = find_invalid_record(data_list, preamble_size)
    contiguous_range = find_contiguous_range(data_list, invalid_record)

    return min(contiguous_range) + max(contiguous_range)


def find_invalid_record(data_list, preamble_size):
    data = data_list[preamble_size:]
    for index in range(len(data)):
        preamble = data_list[index:preamble_size + index]
        if check_preamble(preamble, data[index]):
            return data[index]


def check_preamble(preamble, value):
    for num in preamble:
        if value - num in preamble:
            return False

    return True


def find_contiguous_range(data_list, invalid_record):
    range_list, range_sum, start_index = list(), 0, 0

    while range_sum != invalid_record:
        range_list.clear()
        range_sum = 0

        for val in data_list[start_index:]:
            range_list.append(val)
            range_sum += val

            if range_sum >= invalid_record:
                start_index += 1
                break

    return range_list


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(sum_of_contiguous_range_edges(iohandler.begin(__file__), 25)))
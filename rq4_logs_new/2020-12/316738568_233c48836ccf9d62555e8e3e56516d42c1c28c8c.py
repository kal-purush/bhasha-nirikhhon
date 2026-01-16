from util import iohandler

# --- solution ---


def get_invalid_record(input_file, preamble_size):
    invalid_record = None
    data_list = [int(row.strip()) for row in input_file]

    data = data_list[preamble_size:]
    for index in range(len(data)):
        preamble = data_list[index:preamble_size+index]
        if check_preamble(preamble, data[index]):
            invalid_record = data[index]
            break

    return invalid_record


def check_preamble(preamble, value):
    for num in preamble:
        if value - num in preamble:
            return False

    return True


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(get_invalid_record(iohandler.begin(__file__), 25)))
from util import iohandler

# --- solution ---


def init(input_file):
    d = list()
    for e in input_file:
        d.append(str(e.strip()))
    return d


def sum_of_correct_passwords(database):
    valid_sum = 0
    for policy in database:
        parts = policy.split(' ')
        limits = parts[0].split('-')
        count = int(parts[2].count(parts[1][0]))
        if int(limits[0]) <= count <= int(limits[1]):
            valid_sum += 1

    return valid_sum


# --- solution ---

if __name__ == '__main__':
    input_list = init(iohandler.begin(__file__))
    iohandler.end(str(sum_of_correct_passwords(input_list)))
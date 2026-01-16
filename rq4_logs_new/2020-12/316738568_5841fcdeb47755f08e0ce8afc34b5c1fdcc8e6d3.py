from util import iohandler

# --- solution ---


def init(input_file):
    d = list()
    for e in input_file:
        d.append(str(e.strip()))
    return d


def sum_of_correct_passwords_by_pos(database):
    valid_sum = 0
    for policy in database:
        if build_xor(policy):
            valid_sum += 1

    return valid_sum


def build_xor(pol):
    parts = pol.split(' ')

    positions = parts[0].split('-')
    char = parts[1][0]
    password = parts[2]

    return bool(password[int(positions[0])-1] == char) ^ bool(password[int(positions[1])-1] == char)


# --- solution ---

if __name__ == '__main__':
    input_list = init(iohandler.begin(__file__))
    iohandler.end(str(sum_of_correct_passwords_by_pos(input_list)))
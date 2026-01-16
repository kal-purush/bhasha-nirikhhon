from util import iohandler

# --- solution ---

expected_fields = {'byr', 'iyr', 'eyr', 'hgt', 'hcl', 'ecl', 'pid'}


def init(input_file):
    passports = list()
    passport = str()
    for line in input_file:
        if line != '\n':  # double blank line at the end of input is important
            passport += line.replace('\n', ' ')
        else:
            passports.append(passport.strip())
            passport = ''

    return passports


def num_of_valid_passports(input_file):
    valid_passports = 0
    passports = init(input_file)

    for passport in passports:
        field_list = set()
        for field in passport.split(' '):
            field_list.add(field.split(':', 1)[0])

        field_list.discard('cid')  # discard removes only if present, remove gives error if not present

        if field_list == expected_fields:
            valid_passports += 1

    return valid_passports


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(num_of_valid_passports(iohandler.begin(__file__))))
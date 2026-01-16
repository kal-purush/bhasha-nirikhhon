import re
from util import iohandler

# --- solution ---

expected_fields = {'byr', 'iyr', 'eyr', 'hgt', 'hcl', 'ecl', 'pid'}
valid_byr = [1920, 2002]
valid_iyr = [2010, 2020]
valid_eyr = [2020, 2030]
valid_hgt = [['cm', 150, 193], ['in', 59, 76]]
valid_hcl = re.compile('^#[0-9a-f]{6}$')
valid_ecl = {'amb', 'blu', 'brn', 'gry', 'grn', 'hzl', 'oth'}
valid_pid = re.compile('^[0-9]{9}$')  # ^ and $ says exact matching otherwise it's at least 9 matching


def init(input_file):
    passports = list()
    passport = str()
    for line in input_file:
        if line != '\n':
            passport += line.replace('\n', ' ')
        else:
            passports.append(passport.strip())
            passport = ''

    passports.append(passport.strip())  # handling last line if no nl in the end of file
    return passports


def num_of_valid_passports_by_rules(input_file):
    valid_passports = 0
    passports = init(input_file)

    for passport in passports:
        field_list = set()
        valid_data = True
        for field in passport.split(' '):
            valid_data = validate_value(field.split(':', 1)) and valid_data
            field_list.add(field.split(':', 1)[0])

        field_list.discard('cid')  # discard removes only if present, remove gives error if not present
        if field_list == expected_fields and valid_data:
            valid_passports += 1

    return valid_passports


def validate_value(field):
    ret = False

    # python's switch-case alternatives not really worked with statements
    if 'byr' == field[0]:
        ret = valid_byr[0] <= int(field[1]) <= valid_byr[1]
    elif 'iyr' == field[0]:
        ret = valid_iyr[0] <= int(field[1]) <= valid_iyr[1]
    elif 'eyr' == field[0]:
        ret = valid_eyr[0] <= int(field[1]) <= valid_eyr[1]
    elif 'hgt' == field[0]:
        ret = validate_hgt(field[1])
    elif 'hcl' == field[0]:
        ret = valid_hcl.match(field[1]) is not None
    elif 'ecl' == field[0]:
        ret = field[1] in valid_ecl
    elif 'pid' == field[0]:
        ret = valid_pid.match(field[1]) is not None
    elif 'cid' == field[0]:
        ret = True  # cid
    else:
        ret = False  # invalid field

    return ret


def validate_hgt(hgt_value):
    if len(hgt_value) <= 3:
        return False

    index = 0
    if hgt_value[-2:] == valid_hgt[1][0]:
        index = 1

    return valid_hgt[index][1] <= int(hgt_value[:-2]) <= valid_hgt[index][2]


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(num_of_valid_passports_by_rules(iohandler.begin(__file__))))
"""Convert a starfighter data file in VB6 format to JSON."""

import itertools
import json
import struct
import sys
from vb6_stuff import *


def get_armor_type(arm):
    if arm == 0:
        return 'Standard'
    elif arm == 1:
        return 'Didrate'
    elif arm == 2:
        return 'Trinnium'
    elif arm == 3:
        return 'Tri-Di Composite'
    elif arm == 4:
        return 'Clearplast'
    else:
        raise RuntimeError


def location_name(loc):
    if loc == 1:
        return 'C'
    elif loc == 2:
        return 'F'
    elif loc == 3:
        return 'LW'
    elif loc == 4:
        return 'RW'


# This is taken from itertools documentation.
def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks"""
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def parse_criticals(crit_buf, weapons, engines):
    ret = {
        'C': [],
        'F': [],
        'LW': [],
        'RW': []
    }
    for (loc, rec_num, id_num) in grouper(crit_buf, 3):
        loc_name = location_name(loc)
        if not loc_name:
            continue
        if rec_num in weapons:
            name = 'W' + str(rec_num) + '-' + weapons[rec_num]
            ret[loc_name].append(name)
        elif rec_num < 0 and -rec_num in engines:
            # engine criticals are stored with negative numbers
            name = 'E' + str(-rec_num) + '-' + engines[-rec_num]
            ret[loc_name].append(name)
        else:
            #ret[loc_name].append(rec_num)
            raise RuntimeError
    return ret


def parse_starfighter(filename, weapons, engines):
    with open(filename, 'rb') as f:
        record = struct.unpack('<25s6s94h', f.read())
        return {
            'name': record[0].decode().strip(),
            'abbr': record[1].decode().strip(),
            'space': (record[2] + 2) * 5,
            'criticals': parse_criticals(record[3:86], weapons, engines),
            'armor': {
                'C': record[87],
                'F': record[88],
                'LW': record[89],
                'RW': record[90]
            },
            'armor_type': get_armor_type(record[91]),
            'shields': record[92],
            'speed': record[93],
            'techbase': get_techbase(record[94])[0],
            'wings': record[95]
        }


def load_data_file(filename):
    with open(filename, 'r') as f:
        for obj in json.load(f):
            yield (obj['id'], obj['name'])


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('usage:', 'python', sys.argv[0], '<weapons file> <engines file> <sw2 file>')
        sys.exit(1)

    weapons_file = sys.argv[1]
    weapons = dict(load_data_file(weapons_file))
    engines_file = sys.argv[2]
    engines = dict(load_data_file(engines_file))
    starfighter_file = sys.argv[3]
    starfighter = parse_starfighter(starfighter_file, weapons, engines)
    print(json.dumps(starfighter, indent=4))
from util import iohandler
import time
from functools import reduce

# --- solution ---


def init(input_file):
    return [str(row.strip()) for row in input_file][1]


# refactor based on Chinese Remainder Theory
# https://fangya.medium.com/chinese-remainder-theorem-with-python-a483de81fbb8
def get_earliest_matching_departs(data_list):
    bus_ids = [str(bus_id) for bus_id in data_list.split(',')]
    ids_by_delay = dict()
    for index in range(len(bus_ids)):
        bus_id = bus_ids[index]
        if bus_id != 'x':
            ids_by_delay.update({index: int(bus_id)})

    start = time.time()
    ret = 0

    for num in range(100000000000000, lcm(ids_by_delay.values()), ids_by_delay.pop(0)):

        if time.time() > start + 60:
            print(time.strftime('%X') + " - Processed nums: " + str(num))
            start = time.time()

        found = True
        for key in ids_by_delay.keys():
            if num % ids_by_delay.get(key) != ids_by_delay.get(key) - key:
                found = False
                break

        if found:
            ret = num
            break

    print('FINISHED ' + str(ret))
    return ret


# least common multiple
# this is built in after py 3.9 (math.lcm(integers))
def lcm(num_list):
    multi = 1
    for num in num_list:
        multi *= num

    return abs(multi)


# --- solution ---

if __name__ == '__main__':
    input_ids = init(iohandler.begin(__file__))
    iohandler.end(str(get_earliest_matching_departs(input_ids)))
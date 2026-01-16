from util import iohandler

# --- solution ---


def get_earliest_bus_id(input_file):
    data_list = [str(row.strip()) for row in input_file]
    earliest_depart = int(data_list[0])
    bus_ids = set([int(0 if bus_id == 'x' else bus_id) for bus_id in data_list[1].split(',')])
    bus_ids.discard(0)

    time_to_arrive = dict()
    for bus_id in bus_ids:
        time_to_arrive.update({bus_id - earliest_depart % bus_id: bus_id})

    earliest_time = min(time_to_arrive.keys())
    return earliest_time * time_to_arrive.get(earliest_time)


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(get_earliest_bus_id(iohandler.begin(__file__))))
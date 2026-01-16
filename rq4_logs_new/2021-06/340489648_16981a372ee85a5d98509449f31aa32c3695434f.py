import os
import pathlib


class DataStorage:
    def __init__(self):
        self.base_path = pathlib.PosixPath('~/data/data/').expanduser()

    def exists(self, search_element):
        result_id, agency_id, docket_id = extract_ids(search_element)
        for _, _, files in os.walk(self.base_path / agency_id / docket_id):
            for file in files:
                if file == f'{result_id}.json':
                    return True

        return False


def extract_ids(search_element):
    result_id = search_element['id']

    tokens = result_id.split('-')

    agency_id = tokens[0]

    year = tokens[1]
    docket_counter = tokens[2]
    docket_id = agency_id + '-' + year + '-' + docket_counter

    return result_id, agency_id, docket_id
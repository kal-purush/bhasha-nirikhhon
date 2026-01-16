import json
import logging
logger = logging.getLogger(__name__)

def load(filename):
    with open(filename) as file:
        try:
            data = json.load(file)
            check_data_validity(data, file)
            return data
        except ValueError:
            logger.error('Error parsing json data from file {}'.format(file))
            return False

def check_data_validity(data, file):
    data = data['validation']
    if data[0] != int(data[1]):
        logger.warning('The data is not falidated ({} != {}, {} difference) for {}'.format(data[0], data[1], data[0] - data[1], file))
from logging.config import dictConfig
from logging import Logger

import json

from . import APP


def dict_merge(a, b):
    if not isinstance(b, dict):
        return b
    result = a
    for key in b.keys():
        value = b[key]
        if key in result and isinstance(result[key], dict):
            result[key] = dict_merge(result[key], value)
        else:
            result[key] = value
    return result

resultDict = {}

for f in APP.config['LOGGING_CONFIGS']:
    fh = open(f, 'r')
    newDict = json.load(fh)
    resultDict = dict_merge(resultDict, newDict)

dictConfig(resultDict)

APP.logger: Logger
APP.logger.debug('Debug logging enabled')
APP.logger.info('Connecting to database %s.', APP.config['SQLALCHEMY_DATABASE_URI'])
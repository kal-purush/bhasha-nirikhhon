# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import functools
import time

import obd
import six


MAX_RETRY_COUNT = 5
TIME_INTERVAL = 5

_CONN = None


def get_connection():
    global _CONN
    if _CONN is None:
        _CONN = obd.OBD()
    return _CONN


def retry(f):
    """OBD sometimes returns empty objects. Just retrying usually works."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        for __ in six.moves.range(MAX_RETRY_COUNT):
            result = f(*args, **kwargs)
            if result.message is not None:
                return result
        print('Not supported')
        return None
    return wrapper


@retry
def execute_command(command):
    connection = get_connection()
    response = connection.query(command)
    return response


def fetch_data(command):
    response = execute_command(command)
    if response is not None:
        print(response.value, response.unit)


def consumer_service():
    while True:
        fetch_data(obd.commands.ENGINE_LOAD)
        fetch_data(obd.commands.FUEL_LEVEL)
        fetch_data(obd.commands.FUEL_STATUS)
        fetch_data(obd.commands.FUEL_TYPE)
        fetch_data(obd.commands.INTAKE_PRESSURE)
        fetch_data(obd.commands.RPM)
        fetch_data(obd.commands.SPEED)

        time.sleep(TIME_INTERVAL)
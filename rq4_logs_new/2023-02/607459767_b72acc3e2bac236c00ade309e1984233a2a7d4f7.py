#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from _src._api import logger

logging = logger.logger

# Print header for log in Jenkins
def print_header(text: str, max_width=80):
    separator = '=' * max_width
    logging.info("\n{sep}\n{text}\n{sep}\n".format(
        sep=separator, text=text.center(max_width)))

# Print header for log in Jenkins
def print_method(method):
    def main(*args, **kwargs):
        logging.debug(f'[START] {method.__name__}')
        result = method(*args, **kwargs)
        logging.debug(f'[DONE] {method.__name__}')
        return result
    return main
"""
Main module for start
"""

import logging

from bars_provider import __version__
from bars_provider.downloads import Downloads
from bars_provider.log import logger, setup_logging


if __name__ == '__main__':
  logger.setLevel(logging.DEBUG)
  setup_logging()
  logger.info(__version__)
  Downloads('config.json').startTimer()
import logging
import os

from logging import handlers

from dateutil.utils import today

log = logging.getLogger('server')
PATH = os.path.dirname(os.path.abspath(__file__))
PATH_DAILY_HAND = os.path.join(PATH, f'server{today().strftime("%d-%m-%Y")}.log')
PATH = os.path.join(PATH, 'server.log')
file_hand = logging.FileHandler(PATH, encoding='utf-8')

file_hand_daily = handlers.TimedRotatingFileHandler(PATH_DAILY_HAND, when='D', interval=1, encoding='utf-8')

formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
file_hand.setFormatter(formatter)
log.addHandler(file_hand)
log.addHandler(file_hand_daily)
log.setLevel(logging.DEBUG)
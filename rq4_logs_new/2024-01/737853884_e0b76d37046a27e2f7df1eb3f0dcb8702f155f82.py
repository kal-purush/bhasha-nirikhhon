from datetime import date, time
from enum import Enum


class Duration(Enum):
    THIRTY_MINUTES = "30 минут"
    ONE_HOUR = "1 час"
    GROUP_CALL = "Групповой созвон"
    NOT_SELECTED = "Не выбрано"


class Difficulty(Enum):
    LIGHT = "Лайт"
    STANDARD = "Стандарт"
    HARD = "Хард"
    NOT_SELECTED = "Не выбрано"


class Record:
    date: date
    time: time
    fullname: str
    duration: Duration
    theme: str
    difficulty: Difficulty
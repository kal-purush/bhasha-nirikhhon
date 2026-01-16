import re
from typing import List, Optional

class FileFindCondition(object):
    def __init__(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day
    
    def set_condition(self, year, month, day):
        self.year = year
        self.month = month
        self.day = day
    
    def match(self, file_path:str) -> bool:
        pattern = self._condition_former()
        return re.match(pattern, file_path)

    def _condition_former(self) -> str:
        # try year, month, day to convert to int
        # it may fail because of wildcard, in that case, pass
        # this is to avoid "08" to consider as wildcard
        try:
            _year = int(self.year)
        except:
            _year = self.year
        try:
            _month = int(self.month)
        except:
            _month = self.month
        try:
            _day = int(self.day)
        except:
            _day = self.day

        if isinstance(_year, int):
            _year = "{:04d}".format(_year)
        elif _year == "*":
            _year = "[0-9]{4}"
        if isinstance(_month, int):
            _month = "{:02d}".format(_month)
        elif _month == "*":
            _month = "[0-9]{2}"
        if isinstance(_day, int):
            _day = "{:02d}".format(_day)
        elif _day == "*":
            _day = "[0-9]{2}"
        
        return f'.*{_year}{_month}{_day}.csv'

class ConditionalFileFinder(object):

    def pick_files_from_with(self, file_paths, condition) -> List[str]:
        result = []
        for file_path in file_paths:
            if condition.match(file_path):
                result.append(file_path)
        return result
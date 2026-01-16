#coding=utf8
#!/etc/bin/python 


import re
import json


class LogParser():
    def __init__(self, file_name):
        self.file=file_name

    def __test_file(self):
        raise NotImplementedError

    def initial_target_pattern(self, keys_words):
        raise NotImplementedError

    def extract_target_log_records(self):
        raise NotImplementedError

    def get_statistic(self):
        raise NotImplementedError



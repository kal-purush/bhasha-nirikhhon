import os
from .constants import FIELDS_NAMES


class EquityParser():
    """Take .csv file path as input and return list of dic"""

    def _is_readable_file(self, path):
        return os.path.isfile(path) and os.access(path, os.R_OK)

    def __init__(self):
        self.column_names = FIELDS_NAMES
        self.erros = {}

    def _covert_line_into_dic(self, line, column_names_with_index):
        """Input is line and cloumn names with its index convert it into dic"""
        company_raw_details = line.split(',')
        company_details = {}
        for key, value in column_names_with_index.iteritems():
            company_details[key] = company_raw_details[value].strip()

        return company_details

    def _find_company_colums_with_index(self, columns_in_file):
        """find position of cloumn name """
        column_names_with_index = {}
        for index_num in range(0, len(columns_in_file)):
            if columns_in_file[index_num] in self.column_names:
                column_names_with_index[columns_in_file[index_num]] = index_num

        return column_names_with_index

    def parse(self, path):
        """Take .csv file path as input and return list of dic"""
        companies_details = []
        with open(path, "rb") as finput:
            self.lines = finput.read().split('\n')
            column_names_with_index = self._find_company_colums_with_index(
                self.lines[0].split(','))

            companies_details = [self._covert_line_into_dic(
                line, column_names_with_index
            ) for line in self.lines[1:] if line]
        return companies_details
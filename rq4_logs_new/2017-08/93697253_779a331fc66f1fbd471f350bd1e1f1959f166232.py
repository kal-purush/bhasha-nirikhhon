import os
import sys
import xml.etree.ElementTree as x
from collections import namedtuple

DatabaseConfig = namedtuple('DatabaseConfig', 'machine update')
ReportConfig = namedtuple('ReportConfig', 'output_file_type output_file_name template '
                                          'user')


class ConfigParser(object):

    def __init__(self, file_name):
        self._root = None
        self._parser = None
        self.file_name = file_name

        # Database Config values
        self._machine, self._update = None, True

        # Report Config values
        self._output_file_name, self._output_file_type = None, None
        self._template, self._user = None, None

        # Check for file presence
        if not os.path.isfile(self.file_name):
            print('Could not find file \'{}\''.format(self.file_name))
            sys.exit(-1)

        try:
            self._parser = x.parse(self.file_name)
            self._root = self._parser.getroot()
            self.parse()
        except x.ParseError:
            print('Invalid XML file.')
            sys.exit(-1)

    @property
    def root(self):
        return self._root

    @property
    def parser(self):
        return self._parser

    @property
    def machine(self):
        return self._machine

    @property
    def update(self):
        return self._update

    @property
    def output_file_type(self):
        return self._output_file_type

    @property
    def output_file_name(self):
        return self._output_file_name

    @property
    def template(self):
        return self._template

    @property
    def user(self):
        return self._user

    @property
    def db(self):
        return DatabaseConfig(self._machine, self._update)

    @property
    def report(self):
        return ReportConfig(self._output_file_type, self._output_file_name,
                            self._template, self._user)

    def parse(self):
        db_element = self.root.find('database')

        if db_element is not None:
            if db_element.find('machine') is not None:
                self._machine = db_element.find('machine').text

            update_element = db_element.find('update')
            if update_element is not None:
                if update_element.text == 'true' or update_element.text == '1':
                    self._update = True
                else:
                    self._update = False
            else:
                self._update = True

        report_element = self.root.find('report')

        if report_element is not None:

            if report_element.find('output_file_name') is not None:
                self._output_file_name = report_element.find('output_file_name').text

            if report_element.find('output_file_type') is not None:
                self._output_file_type = report_element.find('output_file_type').text

            if report_element.find('template') is not None:
                self._template = report_element.find('template').text

            if report_element.find('user') is not None:
                self._user = report_element.find('user').text

    def as_dict(self):
        tree = {}

        for element in self.root:
            tree[element.tag] = {}
            for child_element in element:
                tree[element.tag][child_element.tag] = child_element.text

        return tree

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __repr__(self):
        rep = []
        for element in self.root.iter():
            rep.append(element)
        return str(rep)

    def __str__(self):
        return '\'{0}\'{1}'.format(self.file_name, self.as_dict())
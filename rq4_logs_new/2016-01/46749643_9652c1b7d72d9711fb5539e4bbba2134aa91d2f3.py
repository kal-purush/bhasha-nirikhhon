__author__ = 'tuco'

from biomajmanager.plugins import BMPlugin


class anotherplugin(BMPlugin):
    """

    """
    def get_cfg_name(self):
        return self.config.get(self.get_name(), 'anotherplugin.name')

    def get_cfg_value(self):
        return self.config.get(self.get_name(), 'anotherplugin.value')

    def get_value(self):
        return 1

    def get_string(self):
        return "test"

    def get_false(self):
        return False

    def get_true(self):
        return True

    def get_none(self):
        return None

    def get_exception(self):
        return Exception()
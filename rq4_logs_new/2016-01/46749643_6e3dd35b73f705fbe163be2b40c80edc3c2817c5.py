from biomajmanager.plugins import BMPlugin


class Myplugin(BMPlugin):
    """

    """
    def get_cfg_name(self):
        return self.config.get(self.get_name(), 'myplugin.name')

    def get_cfg_value(self):
        return self.config.get(self.get_name(), 'myplugin.value')

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
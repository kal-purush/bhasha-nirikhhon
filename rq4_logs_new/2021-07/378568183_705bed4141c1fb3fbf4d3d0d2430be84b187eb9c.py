import unittest

from fload.execute import load_from_python_module, load_builtin_module

class TestModule:
    pass

class ExecuteTest(unittest.TestCase):
    def test_load_from_python_module(self):
        module_name = 'tests.test_execute:TestModule'
        m = load_from_python_module(module_name)
        self.assertIsNotNone(m)

    def test_load_builtin_module(self):
        module_name = 'csv'
        m = load_builtin_module(module_name)
        self.assertIsNotNone(m)
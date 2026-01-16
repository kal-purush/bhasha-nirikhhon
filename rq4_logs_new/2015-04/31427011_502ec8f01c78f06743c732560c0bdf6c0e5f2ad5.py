import unittest
import importlib

ENGINES = [
    'mayavi_tools']

class TestEngineImport(unittest.TestCase):

    def test_engine_import(self):
        for engine in ENGINES:
            importlib.import_module('simphony.visualisation',engine)
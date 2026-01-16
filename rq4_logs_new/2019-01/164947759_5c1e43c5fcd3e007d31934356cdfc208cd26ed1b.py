"""use unit test to test the scripts in inventory management file"""

import unittest
import sys

from inventory_management.main import *
from inventory_management.electric_appliances_class import *
from inventory_management.furniture_class import *
from inventory_management.inventory_class import *
"""the following sytle didn't make me to be able to put the test file outside of inventory_management folder
from inventory_management import electric_appliances_class
from inventory_management import furniture_class
from inventory_management import inventory_class"""

class ElectricApplicancesClassTest(unittest.TestCase):

    def test_out_put_dict(self):
        example_01 = ElectricAppliances(1, "LED light", 10, 8,"Ikea", "220V")
        dict_01 = example_01.return_as_dictionary()
        dict_01_actual = {'productCode':1, 'description': 'LED light',\
            'marketPrice': 10, \
            'rentalPrice': 8,\
            'brand': "Ikea", \
            'voltage': "220V"}
        self.assertEqual(dict_01, dict_01_actual)

class InventoryClassTest(unittest.TestCase):

    def test_out_put_dict(self):
        example_02 = Inventory(2, "car", 10000, 8888)
        dict_02 = example_02.return_as_dictionary()
        dict_02_actual = {'productCode':2, 'description':"car", 'marketPrice': 10000, \
            'rentalPrice': 8888}
        self.assertEqual(dict_02, dict_02_actual)

class FurnitureClassTest(unittest.TestCase):

    def test_out_put_dict(self):
        example_03 = Furniture(3, "Table", 80, 75, "Wood", "Medium")
        dict_03 = example_03.return_as_dictionary()
        dict_03_actual = {'productCode':3, 'description':"Table",\
            'marketPrice': 80, \
            'rentalPrice': 75,\
            'material': "Wood", \
            'size': "Medium"}
        self.assertEqual(dict_03, dict_03_actual)

class MainTest(unittest.TestCase):

    def test_get_price(self):
        FULL_INVENTORY = {1: {'productCode':1, 'description': 'LED light',\
            'marketPrice': 10, \
            'rentalPrice': 8,\
            'brand': "Ikea", \
            'voltage': "220V"}, \
            2: {'productCode':2, 'description':"car", 'marketPrice': 10000, \
            'rentalPrice': 8888}, \
            3: {'productCode':3, 'description':"Table",\
            'marketPrice': 80, \
            'rentalPrice': 75,\
            'material': "Wood", \
            'size': "Medium"}}
        price_01 = get_price(1, FULL_INVENTORY)
        price_01_actual = 10
        self.assertEqual(price_01, price_01_actual)
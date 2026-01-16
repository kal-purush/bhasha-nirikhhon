# Student: Bradnon Nguyen
# Class:   Advance Python 220 - Jan2019
# Lesson01 - test_integration module
"""
Module: test_integration - to test existing code in inventory package.
"""
from unittest import TestCase
from unittest.mock import MagicMock
from inventory_management.inventory_class import Inventory
from inventory_management.furniture_class import Furniture
from inventory_management.electric_appliances_class import ElectricAppliances


# Make call to all the 3 classes.
class InventoryManagmentModuleTest(TestCase):
    """This class is to test all the class in inventory_management Module."""
    def test_module(self):
        """To integrate test all the classes in inventory_managment module"""
        expected_dict = {1234: {'productCode': 1234,
                                'description': 'This is a base item.',
                                'marketPrice': 24, 'rentalPrice': 25},
                         1235: {'product_code': 1235,
                                'description': 'This is a furniture item.',
                                'market_price': 25, 'rental_price': 26,
                                'material': 'Wood', 'size': 'M'},
                         1236: {'product_code': 1236,
                                'description': 'This is an electric item.',
                                'market_price': 26, 'rental_price': 27,
                                'brand': 'GE', 'voltage': 110}, }
        result_dict = {}
        base_item = Inventory(1234, "This is a base item.", 24, 25)
        result_dict[1234] = base_item.return_as_dictionary()

        furniture_item = Furniture(1235, "This is a furniture item.",
                                   25, 26, "Wood", "M")
        result_dict[1235] = furniture_item.return_as_dictionary()

        electric_item = ElectricAppliances(1236, "This is an electric item.",
                                           26, 27, 'GE', 110)
        result_dict[1236] = electric_item.return_as_dictionary()

        self.assertEqual(expected_dict, result_dict)
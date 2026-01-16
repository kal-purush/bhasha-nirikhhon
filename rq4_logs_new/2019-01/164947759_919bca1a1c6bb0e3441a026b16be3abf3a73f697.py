from unittest import TestCase
from unittest.mock import MagicMock

import inventory_management.inventory_class
#import inventory_management.furniture_class

class TestInventoryManagement(TestCase):
    def test_inventory(self):
        inventory = inventory_management.inventory_class.Inventory(1, '1', '1', '1')

        output_dict = {}
        output_dict['product_code'] = 1
        output_dict['description'] = '1'
        output_dict['market_price'] = '1'
        output_dict['rental_price'] = '1'
        self.assertEqual(output_dict, inventory.return_as_dictionary())

    # def test_furniture(self):
    #     output_dict = {}
    #     output_dict['product_code'] = 1
    #     output_dict['description'] = '1'
    #     output_dict['market_price'] = '1'
    #     output_dict['rental_price'] = '1'
    #     output_dict['material'] = '1'
    #     output_dict['size'] = '1'

    #     furniture = inventory_management.furniture_class.Furniture(*output_dict)
    #     self.assertEqual(output_dict, furniture.return_as_dictionary())
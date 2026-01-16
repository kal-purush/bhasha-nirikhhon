import unittest
from inventory_management.inventory_class import Inventory
from inventory_management.electric_appliances_class import ElectricAppliances
from inventory_management.furniture_class import Furniture
from inventory_management import market_prices
from inventory_management import main
from unittest.mock import patch, Mock, MagicMock
from unittest import mock

class TestInventoryclass(unittest.TestCase):
    def test_return_as_dict(self):
        base_dict = Inventory('a', 'b', 100, 150)
        dict_output = dict()
        dict_output['productCode'] = 'a'
        dict_output['description'] = 'b'
        dict_output ['marketPrice'] = 100
        dict_output ['rentalPrice'] = 150
        self.assertDictEqual(base_dict.return_as_dictionary(), dict_output)

class TestElectric(unittest.TestCase):
    def test_return_as_dict_elec(self):
        electric_dict = ElectricAppliances('a', 'b', 100, 150, 'c', 'd')
        dict_output = dict()
        dict_output['productCode'] = 'a'
        dict_output['description'] = 'b'
        dict_output['marketPrice'] = 100
        dict_output['rentalPrice'] = 150
        dict_output['brand'] = 'c'
        dict_output['voltage'] = 'd'
        self.assertDictEqual(electric_dict.return_as_dictionary(), dict_output)

class TestFurniture(unittest.TestCase):
    def test_return_as_dict_fur(self):
        input_dict = Furniture('a', 'b', 100, 150, 'm', 'z')
        dict_output = dict()
        dict_output['productCode'] = 'a'
        dict_output['description'] = 'b'
        dict_output['marketPrice'] = 100
        dict_output['rentalPrice'] = 150
        dict_output['material'] = 'm'
        dict_output['size'] = 'z'
        self.assertDictEqual(input_dict.return_as_dictionary(),dict_output)

class TestMainmenu(unittest.TestCase):
    def test_main_menu(self):
        optional_prompts = ('a', 'b', 'c')
        for prompts in optional_prompts:
            if prompts == 'a':
                return 'Apple'
            elif prompts == 'b':
                return 'Boy'
            else:
                return 'Cat'
        main.main_menu()
        self.assertEqual(main.main_menu('a'), 'Apple')
        self.assertEqual(main.main_menu('b'), 'Boy')
        self.assertEqual(main.main_menu('c'), 'Cat')


class MainMenu(unittest.TestCase):
    def test_mainmenu_input(self):
        with mock.patch('builtins.input', return_value='1'):
            self.assertEqual(main.main_menu(), main.add_new_item)




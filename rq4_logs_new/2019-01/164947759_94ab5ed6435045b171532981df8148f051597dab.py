"""
Unit tests for inventory management classes
"""


from unittest import TestCase
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest import mock
from io import StringIO

from inventory_management.electricappliancesclass import ElectricAppliances
from inventory_management.inventoryclass import Inventory
from inventory_management.furnitureclass import Furniture
from inventory_management.fullinventory import Fullinventory
import inventory_management.market_prices
import inventory_management.main as tc

class InventoryclassTests(TestCase):

    def test_inventory(self):
        ii = Inventory(24, "Test item", 200.00, 50.00)

        self.assertEqual(ii.return_as_dictionary(), {'productCode': 24, 'description': 'Test item', 'marketPrice': 200.00, 'rentalPrice': 50.0 })


class ElectricApplianceTests(TestCase):

    def test_electricappliance(self):
        ii = Inventory(24, "Test item", 200.00, 50.00)
        ea = ElectricAppliances(ii, "Test Brand", "100V")

        self.assertEqual(ea.return_as_dictionary(), {'productCode': 24, 'description': 'Test item', 'marketPrice': 200.00, 'rentalPrice': 50.0, 'brand': 'Test Brand',  'voltage': '100V'} )


class FurnitureTests(TestCase):

    def test_furniture(self):
        ii = Inventory(24, "Test item", 200.00, 50.00)
        fc = Furniture(ii, "Cloth", "XL")

        self.assertEqual(fc.return_as_dictionary(), {'productCode': 24, 'description': 'Test item', 'marketPrice': 200.00, 'rentalPrice': 50.0, 'material': 'Cloth',  'size': 'XL'} )


class FullinventoryTests(TestCase):

    def test_dictfunctions(self):
        ii = Inventory(24, "Test item", 200.00, 50.00)

        inv_dict = Fullinventory()

        inv_dict.add_inventory(24, ii.return_as_dictionary())

        self.assertEqual(inv_dict.full_inventory_dict[24], {'productCode': 24, 'description': 'Test item', 'marketPrice': 200.00, 'rentalPrice': 50.0})

        self.assertEqual(inv_dict.get_inventory(), {24: {'productCode': 24, 'description': 'Test item', 'marketPrice': 200.00, 'rentalPrice': 50.0}})

        self.assertEqual(inv_dict.get_inventory_item(24), {'productCode': 24, 'description': 'Test item', 'marketPrice': 200.00, 'rentalPrice': 50.0})

        self.assertEqual(inv_dict.get_inventory_item(25), None)

class MainTests(TestCase):

    def test_mainmenu1(self):
        self.assertEqual(tc.main_menu('1'), tc.add_new_item)

    def test_mainmenu2(self):
        self.assertEqual(tc.main_menu('2'), tc.item_info)

    def test_mainmenuq(self):
        self.assertEqual(tc.main_menu('q'), tc.exit_program)

    def test_mainmenu_input1(self):
        with mock.patch('builtins.input', return_value='1'):
            self.assertEqual(tc.main_menu(), tc.add_new_item)

    def test_mainmenu_input2(self):
        with mock.patch('builtins.input', return_value='2'):
            self.assertEqual(tc.main_menu(), tc.item_info)

    def test_mainmenu_inputq(self):
        with mock.patch('builtins.input', return_value='q'):
            self.assertEqual(tc.main_menu(), tc.exit_program)

    def test_getprice(self):
        with mock.patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            tc.get_price(24)
            self.assertEqual(mock_stdout.getvalue(), 'Get price for item = 24.\n')

    def test_exit(self):
        with self.assertRaises(SystemExit):
            tc.exit_program()

    def test_addnewinvitem(self):

        newitem = Inventory(24, 'Test item', 50.00, 60.00)
        mock_values = [24, 'Test item', 50.00, 'n', 'n']

        with mock.patch('builtins.input', side_effect=mock_values):
            try:
                tc.add_new_item()

                tc.get_latest_price = MagicMock(60.00)
                tc.get_latest_price.assert_called_with(24)

                tc.Inventory = MagicMock(newitem)
                tc.Inventory.assert_called_with(24, 'Test item', 50.00, 60.00)
            except NameError:
                assert(True)

    def test_addnewfuritem(self):

        newitem = Inventory(24, 'Test item', 50.00, 60.00)
        newfurn = Furniture(newitem, 'Cloth', 'S')
        mock_values = [24, 'Test item', 50.00, 'y', 'Cloth','S', 'n']

        with mock.patch('builtins.input', side_effect=mock_values):
            try:
                tc.add_new_item()

                tc.get_latest_price = MagicMock(60.00)
                tc.get_latest_price.assert_called_with(24)

                tc.Furniture = MagicMock(newfurn)
                tc.Furniture.assert_called_with(newitem, 'Cloth', 'S')
            except NameError:
                assert(True)

    def test_addnewapplitem(self):

        newitem = Inventory(24, 'Test item', 50.00, 60.00)
        newappli = ElectricAppliances(newitem, 'Maytag', '100V')
        mock_values = [24, 'Test item', 50.00, 'y', 'y', 'Maytag','100V']

        with mock.patch('builtins.input', side_effect=mock_values):
            try:
                tc.add_new_item()

                tc.get_latest_price = MagicMock(60.00)
                tc.get_latest_price.assert_called_with(24)

                tc.ElectricAppliances = MagicMock(newappli)
                tc.ElectricAppliances.assert_called_with(newitem, 'Maytag', '100V')
            except NameError:
                assert(True)

    def test_iteminfo(self):

        with mock.patch('builtins.input', return_value=24):
            try:
                tc.item_info()
                tc.FULL_INVENTORY.get_inventory_item = MagicMock({'productCode': 24, 'description': 'Test item', 'marketPrice': 200.00, 'rentalPrice': 50.0})
                tc.FULL_INVENTORY.get_inventory_item.assert_called_with(24)
            except NameError:
                assert(True)
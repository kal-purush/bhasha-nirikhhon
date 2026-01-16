"""test inventory_management"""

from inventory_management.electric_appliances_class import *
from inventory_management.furniture_class import *
from inventory_management.main import *
from inventory_management.inventory_class import *
from inventory_management.market_prices import *

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

price01 = get_price(1,FULL_INVENTORY)
print(f"the price for product code 1 is {price01}")

add_new_item()
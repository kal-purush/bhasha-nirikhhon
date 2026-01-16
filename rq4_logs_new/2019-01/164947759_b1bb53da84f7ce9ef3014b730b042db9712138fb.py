"""
Launches the user interface for the inventory management system
"""


import sys
from .market_prices import get_latest_price
from .inventoryclass import Inventory
from .furnitureclass import Furniture
from .electricappliancesclass import ElectricAppliances
from .fullinventory import Fullinventory

def main_menu(user_prompt=None):
    """ Method to handle Menu display """

    valid_prompts = {"1": add_new_item,
                     "2": item_info,
                     "q": exit_program}
    options = list(valid_prompts.keys())

    while user_prompt not in valid_prompts:
        options_str = ("{}" + (", {}") * (len(options)-1)).format(*options)
        print("Please choose from the following options ({}):"
              .format(options_str))
        print("1. Add a new item to the inventory")
        print("2. Get item information")
        print("q. Quit")
        user_prompt = input(">")
    return valid_prompts.get(user_prompt)

def get_price(item_code):
    """ Retrieves market price: future """

    print("Get price for item = {}.".format(item_code))

def add_new_item():
    """ Gets item details from user and created new inventory item """

    # global FULL_INVENTORY
    item_code = input("Enter item code: ")
    item_description = input("Enter item description: ")
    item_rental_price = input("Enter item rental price: ")

    # Get price from the market prices module
    item_price = get_latest_price(item_code)

    new_invitem = Inventory(item_code, item_description,
                            item_price, item_rental_price)

    is_furniture = input("Is this item a piece of furniture? (Y/N): ")
    if is_furniture.lower() == "y":
        item_material = input("Enter item material: ")
        item_size = input("Enter item size (S,M,L,XL): ")
        new_item = Furniture(new_invitem, item_material,
                             item_size)
    else:
        is_electric_appliance = input("Is this item an electric"
                                      "appliance? (Y/N): ")
        if is_electric_appliance.lower() == "y":
            item_brand = input("Enter item brand: ")
            item_voltage = input("Enter item voltage: ")
            new_item = ElectricAppliances(new_invitem, item_brand, item_voltage)
        else:
            new_item = new_invitem

    FULL_INVENTORY.add_inventory(item_code, new_item.return_as_dictionary())
    print("New inventory item added")


def item_info(item_code=None):
    """ Retrieve item details from inventory. """

    item_code = input("Enter item code: ")
    item_dict = FULL_INVENTORY.get_inventory_item(item_code)
    if item_dict:
        for key, value in item_dict.items():
            print("{}:{}".format(key, value))
    else:
        print("Item not found.")

def print_item(item):
    """ Generic print if item exists """

    if item:
        print("{}".format(item))
    else:
        print("Item not found.")

def exit_program():
    """ Ends the program. """

    sys.exit()

if __name__ == '__main__':

    FULL_INVENTORY = Fullinventory()

    while True:
        print_item(FULL_INVENTORY.get_inventory())
        main_menu()()
        input("Press Enter to continue...........")
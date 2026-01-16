"""
Class object to handle inventory dictionary
"""


class Fullinventory:
    """ Inventory Dictionary Utiltiy"""

    def __init__(self):
        self.full_inventory_dict = {}

    def add_inventory(self, item_code, item_dict):
        """ Add an item to the inventory dictionary """
        self.full_inventory_dict[item_code] = item_dict

    def get_inventory(self):
        """ Print the inventory dictionary """

        return self.full_inventory_dict

    def get_inventory_item(self, item_code):
        """ Get inventory item """
        if item_code in self.full_inventory_dict:
            return self.full_inventory_dict[item_code]

        return None
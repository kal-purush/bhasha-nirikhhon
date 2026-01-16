# electric_appliances_class
""" Create a subclass to get
an dict containing Electric items by getting basic info from inventory class"""
from inventory_class import Inventory


class ElectricAppliances(Inventory):
    """ Subclass of Inventory class to added more parameters"""

    def __init__(self, product_code, description, market_price,
                 rental_price, brand, voltage):
        Inventory.__init__(self, product_code, description, market_price,
                           rental_price)
        # Creates common instance variables from the parent class
        self.brand = brand
        self.voltage = voltage

    def return_as_dictionary(self):
        """ method to return a complete info of electric items as dict"""
        output_dict = Inventory.return_as_dictionary(self)
        output_dict['brand'] = self.brand
        output_dict['voltage'] = self.voltage

        return output_dict
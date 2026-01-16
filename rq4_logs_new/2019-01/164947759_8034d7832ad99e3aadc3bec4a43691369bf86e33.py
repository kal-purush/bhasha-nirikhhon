"""this file is for electric applicanese class for inventory management"""

# Electric Appliances Class
from inventory_class import Inventory

class ElectricAppliances(Inventory):
    """This class is to creat a dictionary for inventory for electric appliances"""

    def __init__(self, productCode, description, marketPrice, rentalPrice, brand, voltage):
        Inventory.__init__(self, productCode, description, marketPrice, rentalPrice)
        # Creates common instance variables from the parent class
        self.brand = brand
        self.voltage = voltage

    def return_as_dictionary(self):
        """This function is to put the item information into a dict"""
        out_put_dict = {}
        out_put_dict['productCode'] = self.product_code
        out_put_dict['description'] = self.description
        out_put_dict['marketPrice'] = self.market_price
        out_put_dict['rentalPrice'] = self.rental_price
        out_put_dict['brand'] = self.brand
        out_put_dict['voltage'] = self.voltage

        return out_put_dict
    
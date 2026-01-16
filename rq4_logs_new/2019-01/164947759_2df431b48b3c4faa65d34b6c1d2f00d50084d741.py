"""This is furniture_class, a subclass of Inventory"""
# Furniture class
from inventory_class import Inventory

class Furniture(Inventory):
    """This is Furniture class, a sub-class of Inventory"""
    def __init__(self, product_code, description, market_price, rental_price, material, size):
        Inventory.__init__(self, product_code, description, market_price, rental_price)
        # Creates common instance variables from the parent class

        self.material = material
        self.size = size

    def return_as_dictionary(self):
        out_put_dict = {}
        out_put_dict['productCode'] = self.product_code
        out_put_dict['description'] = self.description
        out_put_dict['marketPrice'] = self.market_price
        out_put_dict['rentalPrice'] = self.rental_price
        out_put_dict['material'] = self.material
        out_put_dict['size'] = self.size

        return out_put_dict
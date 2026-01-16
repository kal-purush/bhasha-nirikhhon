"""This is inventory_class"""

# Inventory class
class Inventory:
    """This is parent class, inventory_class, it takes product information
    and create a dict to store information"""
    def __init__(self, product_code, description, market_price, rental_price):
        self.product_code = product_code
        self.description = description
        self.market_price = market_price
        self.rental_price = rental_price

    def return_as_dictionary(self):
        """this method is to store item information and creat a dictionary"""
        out_put_dict = {}
        out_put_dict['productCode'] = self.product_code
        out_put_dict['description'] = self.description
        out_put_dict['marketPrice'] = self.market_price
        out_put_dict['rentalPrice'] = self.rental_price

        return out_put_dict
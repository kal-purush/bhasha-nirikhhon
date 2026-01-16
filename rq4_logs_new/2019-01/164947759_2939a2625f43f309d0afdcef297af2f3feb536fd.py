"""
Electric Appliance Class module
"""


class ElectricAppliances():
    """ Electric Appliance class creates dictionary of appliances """

    def __init__(self, inv_item, brand, voltage):

        self.inv_item = inv_item
        self.brand = brand
        self.voltage = voltage

    def return_as_dictionary(self):
        """ Method to create appliance dictionary. """

        output_dict = self.inv_item.return_as_dictionary()
        output_dict['brand'] = self.brand
        output_dict['voltage'] = self.voltage

        return output_dict
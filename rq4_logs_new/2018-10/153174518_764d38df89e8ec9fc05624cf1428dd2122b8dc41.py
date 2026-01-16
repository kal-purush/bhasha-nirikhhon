from exception.invalid_pincode_exception import InvalidPincodeException
from helpers.database import DatabaseHelper

class Pincode(object):
    def __init__(self, pincode):
        self.pincode = pincode
        self.db = DatabaseHelper('pincodes')

    def save(self):
        pincode_from_database = self.db.find_one("pincode = '%s'" % self.pincode)

        if pincode_from_database:
            raise InvalidPincodeException('InvalidPincodeException')

        self.db.insert(dict(pincode=self.pincode))
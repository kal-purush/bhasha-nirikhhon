import iso6346

class ShippingContainer:
    next_serial = 1337

#    @staticmethod
#    def _get_next_serial():
#        result = ShippingContainer.next_serial
#        ShippingContainer.next_serial += 1
#        return result

    @classmethod
    def _get_next_serial(cls):
        result = cls.next_serial
        cls.next_serial += 1
        return result

    @classmethod
    def create_empty(cls, owner_code):
        return cls(owner_code, contents=None)

    @classmethod
    def create_with_items_list(cls, owner_code, items):
        return cls(owner_code, list(items))

    @classmethod
    def create_with_items_parameters(cls, owner_code, *args):
        return cls(owner_code, list(args))

    @staticmethod
    def

    def __init__(self, owner_code, contents):
        self.owner_code = owner_code
        self.contents = contents
        self.serial = ShippingContainer._get_next_serial()
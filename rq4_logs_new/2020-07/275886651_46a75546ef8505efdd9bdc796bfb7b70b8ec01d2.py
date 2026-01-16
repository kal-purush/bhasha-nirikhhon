'''Defines the model for updates'''
import json
from SchoolSystem.data.logger import get_logger

_log = get_logger(__name__)

class Update:
    '''A class that defines how Updates should behave'''
    def __init__(self, db_id=-1, username='', update_info={}):
        self._id = db_id
        self.username = username
        self.update_info = update_info


    def get_id(self):
        '''Returns the id of the update'''
        return self._id

    def set_id(self, _id):
        '''Sets the id of the update'''
        self._id = _id

    def __str__(self):
        '''String representation of the update'''
        string = "_id: " + str(self._id) + " name: " + self.name
        string += " Instance of: " + type(self).__name__
        return string

    def __repr__(self):
        '''Returns string representation of self'''
        return self.__str__()

    def to_dict(self):
        '''Returns the dictionary representation of itself'''
        return self.__dict__

    @classmethod
    def from_dict(cls, input_update):
        '''Creates an instance of the class from a dictionary'''
        update = Update()
        update.__dict__.update(input_update)
        return update

class UpdateEncoder(json.JSONEncoder):
    ''' Allows us to serialize our objects as JSON '''
    def default(self, o):
        return o.to_dict()
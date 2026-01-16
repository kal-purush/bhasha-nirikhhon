'''Defines the model for users'''
import json
import jwt
import datetime

from SchoolSystem.data.logger import get_logger

_log = get_logger(__name__)
_secret_key = '101010101unique'

class Messager:
    '''A class that defines how Assignments should behave'''
    def __init__(self, db_id=-1, msg_to='', msg_from='', msg_msg='', msg_date=''):
        self._id = db_id
        self.msg_to = msg_to
        self.msg_from = msg_from
        self.msg_message = msg_msg
        self.msg_date = msg_date

    def to_dict(self):
        '''Returns the dictionary representation of itself'''
        return self.__dict__

    @classmethod
    def from_dict(cls, input_messager):
        '''Creates an instance of the class from a dictionary'''
        messager = Messager()
        messager.__dict__.update(input_messager)
        return messager


class MessagerEncoder(json.JSONEncoder):
    ''' Allows us to serialize our objects as JSON '''
    def default(self, o):
        return o.to_dict()
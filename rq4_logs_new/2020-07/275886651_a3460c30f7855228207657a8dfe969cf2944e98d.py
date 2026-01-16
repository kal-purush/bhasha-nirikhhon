'''Defines the model for schedule'''
import json
from SchoolSystem.data.logger import get_logger

_log = get_logger(__name__)

class Schedule:
    '''A ClassScheduleSchedule that defines how schedules should behave'''
    def __init__(self, db_id=-1, username='', schedule={'period_1': '', 'period_2': '', 'period_3': '', 'period_4': '', 'period_5': ''}):
        self._id = db_id
        self.username = username
        self.schedule = schedule

    def get_id(self):
        '''Returns the id of the Schedule'''
        return self._id

    def set_id(self, _id):
        '''Sets the id of the Schedule'''
        self._id = _id

    def __str__(self):
        '''String representation of the Schedule'''
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
    def from_dict(cls, input_schedule):
        '''Creates an instance of the Schedule from a dictionary'''
        schedule = Schedule()
        schedule.__dict__.update(input_update)
        return schedule

class ScheduleEncoder(json.JSONEncoder):
    ''' Allows us to serialize our objects as JSON '''
    def default(self, o):
        return o.to_dict()
import pickle
from datetime import datetime


class Event:

    def __init__(self):
        self.id = id
        self.date = datetime.now()
        self.location = ""
        self.event_text = ""


class Scheduler:

    def __init__(self):
        self.__event_list = []
        self.__file_path = 'schedule.dat'
        self.load()

    def load(self):
        try:
            with open(self.__file_path, 'rb') as f:
                self.__event_list = pickle.load(f)
        except:
            self.__event_list = []

    def save(self):
        with open(self.__file_path, 'wb') as f:
            pickle.dump(self.__event_list, f)

    def delete_event(self, id):
        self.__event_list = [x for x in self.__event_list if x.id != id]

    def add_event(self, date, location, event_text):
        new_id = len(self.__event_list) + 1
        self.__event_list.append(Event(new_id, date, location, event_text))

    def get_events(self):
        return self.__event_list



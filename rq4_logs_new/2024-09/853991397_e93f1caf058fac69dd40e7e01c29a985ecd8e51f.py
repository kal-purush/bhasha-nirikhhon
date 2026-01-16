from event import AllDayEvent, TimedEvent
from date_time_utils import get_date, get_time

class EventEdit:
    def __init__(self):
        pass
    
    def event_editor(self, events_dict, event_index):
        edit_event_id = input("Enter the event ID of the event that you wish to edit: ")

        if edit_event_id not in event_index:
            print(f"No event with ID '{edit_event_id}' was found.")
            return events_dict

        date, event = event_index[edit_event_id]

        new_event_name = input("Enter new event name. If none, press enter: ") or event.name
        new_event_desc = input("Enter new event description. If none, press enter: ") or event.description

        new_start_date = get_date("Enter new event start date. If none, press enter: ") or event.start_date
        new_end_date = get_date("Enter new event end date. If none, press enter: ") or event.end_date

        all_day_prompt = input("Will this be an all day event or not? ").lower() or 'No'

        if all_day_prompt == 'yes':
            new_event_data = self.all_day(edit_event_id, new_event_name, new_event_desc,
                                                    new_start_date, new_end_date)
        else:
            new_event_data = self.timed(edit_event_id, new_event_name, new_event_desc,
                                                  new_start_date, new_end_date, event)

        events_dict[date][edit_event_id] = new_event_data

        print(f"Event with ID '{edit_event_id}' has been edited successfully.")

        return events_dict

    @staticmethod
    def all_day(edit_event_id, new_event_name, new_event_desc,
                        new_start_date, new_end_date):
        all_day = True
        return AllDayEvent(edit_event_id, new_event_name, new_event_desc,
                           all_day, new_start_date, new_end_date)

    @staticmethod
    def timed(edit_event_id, new_event_name, new_event_desc,
                        new_start_date, new_end_date, event):
        all_day = False
        new_start_time = get_time("Enter new event start time (HH:MM format): ") or event.start_time
        new_end_time = get_time("Enter new event end time (HH:MM format): ") or event.end_time
        return TimedEvent(edit_event_id, new_event_name, new_event_desc,
                          all_day, new_start_date, new_end_date,
                          new_start_time, new_end_time)
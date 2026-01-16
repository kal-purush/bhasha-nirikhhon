import os
from dotenv import load_dotenv
import caldav
from icalendar import Calendar, Event, Timezone, Todo, Journal


load_dotenv()

# Initialize the CalDav client
caldav_client = caldav.DAVClient(url=os.getenv('CALDAV_SERVER'), username=os.getenv('CALDAV_USER'), password=os.getenv('CALDAV_PASSWORD'))
principal = caldav_client.principal()


# Get the calendar you'll be working with
calendars = principal.calendars()

    
# Function to create an event on the calendar
def cal_create_event(calendar_num, summary, start, end, description, location, category, rrule):
    calendar = calendars[calendar_num]

    # Create the event
    event = Event()
    timezone = Timezone()
    timezone.add('tzid', 'America/New_York')
    event.add_component(timezone)
    if summary is not None:
        event.add('summary', summary)
    if start is not None:
        event.add('dtstart', start)
    if end is not None:
        event.add('dtend', end)
    if description is not None:
        event.add('description', description)
    if location is not None:
        event.add('location', location)
    if category is not None:
        event.add('category', category)
    if rrule is not None:
        event.add('rrule', rrule)
    
     # Create the event on the CalDav server
    try:
        calendar.add_event(event.to_ical())
    except Exception as e:
        print(f"An error occurred: {e}")
        raise


# Function to list events on the calendar
def cal_list_events(calendar_num, start_datetime, end_datetime):
    calendar = calendars[calendar_num]
    # Search for events within a time range
    results = calendar.date_search(start=start_datetime, end=end_datetime)
    # Process the results and return them
    events = []
    for event in results:
        ical_event = Calendar.from_ical(event.data)
        for component in ical_event.walk():
            if component.name == "VEVENT":
                events.append({
                    'url': event.url,
                    'summary': component.get('summary'),
                    'start': component.get('dtstart').dt,
                    'end': component.get('dtend').dt,
                    'time': component.get('dtstamp').dt,
                    'description': component.get('description')
                })
    return events

# Usage example:
# create_event('Meeting with Bob', datetime(2023, 11, 8, 15, 0), datetime(2023, 11, 8, 16, 0), 'Discuss project milestones.')
# events = list_events(datetime.now(), datetime.now() + timedelta(days=7))

# Function to create a todo on the calendar
def cal_create_todo(calendar_num, summary, due, percent, priority, status, description, location, category, rrule):
    calendar = calendars[calendar_num]
    # Create the todo
    todo = Todo()
    timezone = Timezone()
    timezone.add('tzid', 'America/New_York')
    todo.add_component(timezone)
    if summary is not None:
        todo.add('summary', summary)
    if due is not None:
        todo.add('due', due)
    if percent is not None:
        todo.add('percent', percent)
    if priority is not None:
        todo.add('priority', priority)
    if status is not None:
        todo.add('status', status)
    if description is not None:
        todo.add('description', description)
    if location is not None:
        todo.add('location', location)
    if category is not None:
        todo.add('category', category)
    if rrule is not None:
        todo.add('rrule', rrule)
    
     # Create the todo on the CalDav server
    try:
        calendar.add_todo(todo.to_ical())
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def cal_list_tasks(calendar_num):
    calendar = calendars[calendar_num]
    results = calendar.todos()

    tasks = []
    for task in results:
        ical_event = Calendar.from_ical(task.data)
        for component in ical_event.walk():
            if component.name == "VTODO":
                tasks.append({
                    'url': task.url,
                    'uid': component.get('uid'), 
                    'summary': component.get('summary'),
                    'due': component.get('due'),
                    'percent': component.get('percent'),
                    'priority': component.get('priority'),
                    'status': component.get('status'),
                    'description': component.get('description'),
                    'location': component.get('location'),
                    'category': component.get('category'),
                    'rrule': component.get('rrule')
                })
    return tasks

def cal_update_task(calendar_num, url, summary, due, percent, priority, status, description, location, category, rrule):
    calendar = calendars[calendar_num]
    # Get the task by URL
    task = calendar.todo_by_url(url)
    if task is None:
        raise ValueError("Task not found")
    
    # Update the task properties
    ical_event = Calendar.from_ical(task.data)
    for component in ical_event.walk():
        if component.name == "VTODO":
            if summary is not None:
                component.set('summary', summary)
            if due is not None:
                component.set('due', due)
            if percent is not None:
                component.set('percent', percent)
            if priority is not None:
                component.set('priority', priority)
            if status is not None:
                component.set('status', status)
            if description is not None:
                component.set('description', description)
            if location is not None:
                component.set('location', location)
            if category is not None:
                component.set('category', category)
            if rrule is not None:
                component.set('rrule', rrule)
    
    # Update the task on the CalDav server
    try:
        task.set_data(ical_event.to_ical())
        task.save()
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def cal_delete_task(calendar_num, url):
    calendar = calendars[calendar_num]
    # Get the task by URL
    task = calendar.todo_by_url(url)
    if task is None:
        raise ValueError("Task not found")
    
    # Delete the task from the CalDav server
    try:
        task.delete()
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def cal_update_event(calendar_num, url, summary, start, end, description, location, category, rrule):
    calendar = calendars[calendar_num]
    # Get the event by URL
    event = calendar.event_by_url(url)
    if event is None:
        raise ValueError("Event not found")
    
    # Update the event properties
    ical_event = Calendar.from_ical(event.data)
    for component in ical_event.walk():
        if component.name == "VEVENT":
            if summary is not None:
                component.set('summary', summary)
            if start is not None:
                component.set('dtstart', start)
            if end is not None:
                component.set('dtend', end)
            if description is not None:
                component.set('description', description)
            if location is not None:
                component.set('location', location)
            if category is not None:
                component.set('category', category)
            if rrule is not None:
                component.set('rrule', rrule)
    
    # Update the event on the CalDav server
    try:
        event.set_data(ical_event.to_ical())
        event.save()
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

def cal_delete_event(calendar_num, url):
    calendar = calendars[calendar_num]
    # Get the event by URL
    event = calendar.event_by_url(url)
    if event is None:
        raise ValueError("Event not found")
    
    # Delete the event from the CalDav server
    try:
        event.delete()
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
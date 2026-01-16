import datetime
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from datetime import datetime

# If modifying these scopes, delete the file token.json.
SCOPES = "https://www.googleapis.com/auth/calendar"
CAL_ID = "bjbks99te6h18vdpdkkq9mqp4g@group.calendar.google.com"

def auth():
    """
    Auths with the Google API and returns a service resource to query the API
    with.
    """
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('calendar', 'v3', http=creds.authorize(Http()))
    return service

def get_existing_events(service):
    """
    Returns a list of events currently on the calendar using the calendar ID
    """
    events_result = service.events().list(calendarId=CAL_ID).execute()
    existing_events = events_result.get('items', [])
    return existing_events

def filter_existing_events(existing_events, queried_events):
    """
    Given a list of existing events on the Calendar, remove these from the
    queried list from the BSA site so events aren't duplicated on the calendar!
    """

    # Some what lazy, but list sizes are small (< 10)
    for existing_event in existing_events:
        for queried_event in queried_events:
            if existing_event['summary'] == queried_event['summary']:
                queried_events.remove(queried_event)

    return queried_events

def insert_event(service, event):
    """
    Inserts a single event to the calendar
    """
    service.events().insert(calendarId=CAL_ID,body=event).execute()

def delete_event(service, event):
    """
    Deletes a single event currently in the calendar using the event id
    """
    service.events().delete(calendarId=CAL_ID,eventId=event['id']).execute()

def manipulate_events(queried_events):
    """
    Handles interaction with the Calendar API given a list of scrapped events
    """

    # Auth with the API
    service = auth()

    # Get all existing events on the calendar
    existing_events = get_existing_events(service)

    # Remove these from the scrapped events
    filtered_events = filter_existing_events(existing_events, queried_events)

    # Insert events otherwise report there are no events to add
    if filtered_events:
        [ insert_event(service, event) for event in filtered_events ]
        print("Finished. Please wait for the Calendar to update :)")
    else:
        print("No events to add the calendaer :)")
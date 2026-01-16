from __future__ import print_function
import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

import datetime, time
from datetime import timedelta

import pandas

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/calendar-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/calendar'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Calendar API Python Quickstart'


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'calendar-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def main():
    """Shows basic usage of the Google Calendar API.

    Creates a Google Calendar API service object and outputs a list of the next
    10 events on the user's calendar.
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('calendar', 'v3', http=http)

    now = datetime.datetime.now() # 'Z' indicates UTC time

    now = now.isoformat() + 'Z'

    get_list_of_future_events = 0
    if get_list_of_future_events:
        print('Getting the upcoming 10 events')
        eventsResult = service.events().list(
            calendarId='primary', timeMin=now, maxResults=10, singleEvents=True,
            orderBy='startTime').execute()
        events = eventsResult.get('items', [])

        if not events:
            print('No upcoming events found.')
        for event in events:
            start = event['start'].get('dateTime', event['start'].get('date'))
            print(start, event['summary'])

    # Reading 333 events
    birthdays = pandas.DataFrame().from_csv('birthdays.csv')

    event_count = 0
    for date, row in birthdays.iterrows():
        name = row['name']

        if str(name) != 'nan':
            print(date)
            print(name)

            start_time = date + timedelta(days=1461, hours=-5)
            end_time = date + timedelta(days=1461, hours=-4)

            print(start_time)
            print(end_time)     

            start_time = start_time.isoformat() + 'Z'
            end_time = end_time.isoformat() + 'Z'

            # ---- Creating a new event ----
            event = {
                'summary': name,
                'location': 'India',
                'description': 'birthdays',
                'start': {
                    'dateTime': start_time,
                    'timeZone': 'America/New_York',
                },
                'end': {
                    'dateTime': end_time,
                    'timeZone': 'America/New_York',
                },
                'recurrence': [
                    'RRULE:FREQ=YEARLY;COUNT=100'
                ],
            }

            event = service.events().insert(calendarId='primary', body=event).execute()
            event_count += 1

            print(str(event_count) + ' th event created: %s' % (event.get('htmlLink')))
        else:
            print('hehe')




if __name__ == '__main__':
    main()
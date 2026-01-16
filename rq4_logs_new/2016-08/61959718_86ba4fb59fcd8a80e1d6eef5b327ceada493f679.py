
import os
from datetime import datetime

import oauth2client
from exchangelib import EWSTimeZone, EWSDateTime, AllProperties
from exchangelib.folders import CalendarItem
from googleapiclient.errors import HttpError
from oauth2client import client
from oauth2client import tools


FIRST_CELL_MINUTES_AFTER_MIDNIGHT = 7 * 60

SCOPES = ['https://www.googleapis.com/auth/calendar',
          'https://www.googleapis.com/auth/spreadsheets.readonly']

CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Frontline Calendar'
ABBY_ALI_LUNCH = "ABBY_ALI_LUNCH"


class Range:
    start = 0
    end = 0

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def length(self):
        return self.end - self.start

    def subtract(self, other_range):
        # other_range does not overlap at all
        if other_range.end <= self.start or other_range.start >= self.end:
            return [self]
        # other_range complete overlaps
        if other_range.start <= self.start and other_range.end >= self.end:
            return [Range(self.start, self.start)]
        # other_range overlaps beginning
        if other_range.start <= self.start and other_range.end <= self.end:
            return [Range(other_range.end, self.end)]
        # other_range overlaps end
        if other_range.start >= self.start and other_range.end >= self.end:
            return [Range(self.start, other_range.start)]
        # other_range overlaps inner part
        if other_range.start >= self.start and other_range.end <= self.end:
            return [Range(self.start, other_range.start), Range(other_range.end, self.end)]

    def __repr__(self):
        return "{0} - {1}".format(self.start, self.end)


class Appointment:
    start_time = 0
    end_time = 0
    appointment_type = None

    def __init__(self, start_time, end_time, appointment_type):
        self.start_time = start_time
        self.end_time = end_time
        self.appointment_type = appointment_type

    def __repr__(self):
        return "{0}: {1} - {2}".format(self.appointment_type, self.start_time, self.end_time)


def time_from_cell_index(index, day):
    minutes = FIRST_CELL_MINUTES_AFTER_MIDNIGHT + (index * 15)
    return day.replace(minutes=minutes)


def appointment_summary(appointment):
    summary = None
    if appointment.appointment_type == 'F':
        summary = 'On Phones'
    elif appointment.appointment_type == 'C':
        summary = 'On Chat'
    elif appointment.appointment_type == ABBY_ALI_LUNCH:
        summary = 'Abby and Ali Lunch Date'

    return summary


def appointments_from_google_sheet(service, spreadsheet_id, row, midnight):
    appointments = []
    rangeName = "'{date}'!K{row}:BF{row}".format(row=row, date=midnight.strftime("%a %m.%d.%y"))

    try:
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rangeName).execute()
    except HttpError:
        print("Could not find cells on spreadsheet in range {0}".format(rangeName))
        return appointments

    time_blocks = result.get('values', [])
    # shed outer list
    if isinstance(time_blocks, list):
        time_blocks = time_blocks[0]

    currentAppointment = None
    for i in range(0, len(time_blocks)):
        time_block_type = time_blocks[i]
        # if there is an appointment in progress of the same type, extend it
        if currentAppointment and currentAppointment.appointment_type == time_block_type:
            currentAppointment.end_time = time_from_cell_index(i+1, midnight)
        # start a new appointment
        else:
            if currentAppointment:
                appointments.append(currentAppointment)
            currentAppointment = Appointment(time_from_cell_index(i, midnight), time_from_cell_index(i+1, midnight), time_block_type)

    # clean up the last appointment
    appointments.append(currentAppointment)

    return appointments



def create_google_calendar_events(appointments, google_calendar_service):
    events_made = 0
    for appointment in appointments:
        summary = appointment_summary(appointment)

        if summary:
            if not google_calendar_event_exists(appointment, google_calendar_service, summary):
                create_google_calendar_event(appointment, google_calendar_service, summary)
            events_made += 1

    return events_made


def google_calendar_event_exists(appointment, calendar_service, summary):
    matching_events = calendar_service.events().list(calendarId='primary',
                                                     timeMin=appointment.start_time.datetime.isoformat(),
                                                     timeMax=appointment.end_time.datetime.isoformat(),
                                                     q=summary).execute()
    if matching_events and matching_events['items']:
        print("Found {count} matching Google Calendar events for appointment {app}. Will not create a new one.".format(count=len(matching_events['items']), app=appointment))
        return True

    return False


def create_google_calendar_event(appointment, calendar_service, summary):
    event = {
        'summary': summary,
            'start': {
                'dateTime': appointment.start_time.datetime.isoformat()
            },
            'end': {
                'dateTime': appointment.end_time.datetime.isoformat()
            },
        'reminders': {
            'useDefault': False,
            'overrides': [
                {
                    'method': 'popup',
                    'minutes': '5'
                },
                {
                    'method': 'popup',
                    'minutes': '0'
                }
            ]
        },
        'description': 'This event was created by Frontline Calendar. Contact Abby Lance with issues.'
    }

    event = calendar_service.events().insert(calendarId='primary', body=event).execute()
    print('Google Calendar event created. Link: {0} Details: {1}'.format(event.get('htmlLink'), event))


def create_outlook_calendar_events(appointments, outlook_calendar_service):
    events_made = 0
    for appointment in appointments:
        summary = appointment_summary(appointment)

        if summary:
            if not outlook_calendar_event_exists(appointment, outlook_calendar_service, summary):
                create_outlook_calendar_event(appointment, outlook_calendar_service, summary)
            events_made += 1

    return events_made


def outlook_calendar_event_exists(appointment, calendar_service, summary):
    ews_tz = EWSTimeZone.timezone('America/Chicago')

    d = appointment.start_time.datetime
    start_date_time = datetime(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond, ews_tz)
    start_ews_date_time = EWSDateTime.from_datetime(start_date_time)

    d = appointment.end_time.datetime
    end_date_time = datetime(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond, ews_tz)
    end_ews_date_time = EWSDateTime.from_datetime(end_date_time)

    matching_events = calendar_service.calendar.find_items(
        start=start_ews_date_time,
        end=end_ews_date_time,
        shape=AllProperties)

    if matching_events:
        for event in matching_events:
            if event.subject == summary:
                print("Found a matching Outlook calendar event for appointment {app}. Will not create a new one.".format(app=appointment))
                return True

    return False


def create_outlook_calendar_event(appointment, calendar_service, summary):
    ews_tz = EWSTimeZone.timezone('America/Chicago')

    d = appointment.start_time.datetime
    start_date_time = datetime(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond, ews_tz)
    start_ews_date_time = EWSDateTime.from_datetime(start_date_time)

    d = appointment.end_time.datetime
    end_date_time = datetime(d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond, ews_tz)
    end_ews_date_time = EWSDateTime.from_datetime(end_date_time)

    event = CalendarItem(
        subject=summary,
        body='This event was created by Frontline Calendar. Contact Abby Lance with issues.',
        start=start_ews_date_time,
        end=end_ews_date_time
    )

    calendar_service.calendar.add_items([event])
    print('Outlook calendar event created. Details: {0}'.format(event))


def get_credentials(flags):
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, 'drive-python-frontline-calendar.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        flags.noauth_local_webserver = True
        credentials = tools.run_flow(flow, store, flags)
        print('Storing credentials to ' + credential_path)
    return credentials
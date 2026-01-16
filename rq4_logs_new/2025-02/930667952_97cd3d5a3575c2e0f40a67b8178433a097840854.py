import csv
from datetime import datetime, timedelta


def parse_contact_dates(row: dict) -> tuple[str, dict]:
    name = f"{row['First Name']} {row['Last Name']}".strip()
    dates = {}
    if row.get('Birthday'):
        dates['Birthday'] = row.get('Birthday')
    if row.get('Event 1 - Label') and row.get('Event 1 - Value'):
        dates[row.get('Event 1 - Label')] = row.get('Event 1 - Value')
    if row.get('Event 2 - Label') and row.get('Event 2 - Value'):
        dates[row.get('Event 2 - Label')] = row.get('Event 2 - Value')

    return name, dates


def create_ics_event(name, date: str, event):
    dt = datetime.strptime(date.replace(" --", "2024-"), "%Y-%m-%d")
    start = dt.strftime("%Y%m%d")
    end = (dt + timedelta(days=1)).strftime("%Y%m%d")

    ics_event = f"""BEGIN:VEVENT
SUMMARY:{event} - {name}
DTSTART;VALUE=DATE:{start}
DTEND;VALUE=DATE:{end}
RRULE:FREQ=YEARLY
END:VEVENT
"""
    return ics_event


def csv_to_ics(csv_filename, ics_filename):
    with open(csv_filename, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        contacts_with_dates = [parse_contact_dates(row) for row in csv_reader]
        events = [create_ics_event(name, date, event)
                  for name, dates in contacts_with_dates
                  for event, date in dates.items()]

    with open(ics_filename, mode='w', encoding='utf-8') as ics_file:
        ics_file.write("BEGIN:VCALENDAR\nVERSION:2.0\n")
        ics_file.writelines(events)
        ics_file.write("END:VCALENDAR")


csv_to_ics('contacts.csv', 'birthdays_calendar.ics')
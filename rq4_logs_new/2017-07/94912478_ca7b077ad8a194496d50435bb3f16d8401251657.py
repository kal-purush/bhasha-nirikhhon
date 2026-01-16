#!/usr/bin/env python3

from icalendar import Calendar
import datetime as dt
import urllib.request
from operator import itemgetter


def get_cal(direction=False):
    if direction == 'North':
        url = 'https://calendar.google.com/calendar/ical/ik3r8hp2qds6g7247jr09nhoas%40group.calendar.google.com/public/basic.ics'
    elif direction == 'South':
        url = 'https://calendar.google.com/calendar/ical/mk1thfp53sqnlcemg4o1d0vua0%40group.calendar.google.com/public/basic.ics'
    else:
        url = 'https://calendar.google.com/calendar/ical/ik3r8hp2qds6g7247jr09nhoas%40group.calendar.google.com/public/basic.ics'
    with urllib.request.urlopen(url) as cal:
        return Calendar.from_ical(cal.read())


def build_response(output, should_end_session):
    return {
        'version': '1.0',
        'sessionAttributes': {},
        'response': build_speech_response(output, should_end_session)
    }


def build_speech_response(output, should_end_session):
    return {
        'outputSpeech': {
            'type': 'PlainText',
            'text': output
        },
        'card': {
            'type': 'Simple',
            'title': 'Saratoga Garbage',
            'content': output
        },
        'reprompt': {
            'outputSpeech': {
                'type': 'PlainText',
                'text': None
            }
        },
        'shouldEndSession': should_end_session
    }


def build_output_speech(_events):
    if len(_events) == 2:
        return "The next {} is {} and {} is {}".format(_events[0]['summary'], _events[0]['date'], _events[1]['summary'], _events[1]['date'])
    elif len(_events) == 1:
        return "The next {} is {}".format(_events[0]['summary'], _events[0]['date'])
    elif len(_events) == 0:
        return "No days within the next week."
    else:
        _out = 'The next '
        for x in _events:
            _out = _out + "{} is {} ".format(x['summary'], x['date'])
        return _out


def get_day():
    today = dt.date.today()
    one_week = (today + dt.timedelta(days=7))
    cal = get_cal()
    events = cal.walk('vevent')
    _week = [{'summary': event.decoded('summary').decode(), 'date': event.decoded('dtstart')} for event in events if today <= event.decoded('dtstart') <= one_week]
    sorted_week = sorted(_week, key=itemgetter('date'))
    if len(sorted_week) > 2:
        sorted_week = sorted_week[:2]
    output = build_output_speech(sorted_week)
    return build_response(output, True)


def on_intent(request):
    intent = request['intent']
    intent_name = intent['name']

    if intent_name == 'GetDay':
        return get_day()
    elif intent_name in ['AMAZON.StopIntent', 'AMAZON.CancelIntent']:
        return build_response("Goodbye", True)
    elif intent_name == 'AMAZON.HelpIntent':
        return build_response("You can ask me when is garbage day or you can say exit... What can I help you with?", False)


def handler(event, context):
    # Placeholder to verify request
    if False:  # (event['session']['application']['applicationId'] != "amzn1.echo-sdk-ams.app.bd304b90-xxxx-xxxx-86ae-1e4fd4772bab"):
        raise ValueError("Invalid Application ID")

    if event['request']['type'] == 'IntentRequest':
        return on_intent(event['request'])
    if event['request']['type'] == 'LaunchRequest':
        return get_day()
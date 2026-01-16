import requests
import requests_cache
import bs4
import datetime
import json
from icalendar import Calendar, Event  # icalendar==3.9.0

requests_cache.configure('cache_database', expire_after=60*60)

headers = {'Content-Type': 'text/calendar; charset=utf-8',
    'Content-Disposition': 'inline; filename=calendar.ics'}


def generate_calendar():
    req = requests.get('http://bezpieczna.um.warszawa.pl/imprezy-masowe/zgromadzenia')
    soup = bs4.BeautifulSoup(req.text)
    trs = soup.find('table', attrs={'class': 'ViewsTable'}).findAll('tr')
    label = [x.text.strip() for x in trs[0].findAll('th')]

    cal = Calendar()
    cal.add('prodid', '-//Zgromadzenia publiczne w Warszawie//jawne.info.pl//')
    cal.add('version', '0.1.0')

    for tr in trs[1:]:
        date_string = tr.find('td').text.strip()
        date = datetime.datetime.strptime(date_string, '%Y-%m-%d').date()

        values = [x.text.strip() for x in tr.findAll('td')]
        text = json.dumps(dict(zip(label, values)), indent=4)

        event = Event()
        event.add('summary', text)
        event.add('dtstart', date)
        event.add('dtend', date)
        cal.add_component(event)

    return cal.to_ical()


def application(environ, start_response):
    start_response('200 OK', headers.items())
    return [generate_calendar()]

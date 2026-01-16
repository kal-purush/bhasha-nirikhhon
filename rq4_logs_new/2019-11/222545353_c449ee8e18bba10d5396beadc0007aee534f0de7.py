import datetime
import re
import requests
from bs4 import BeautifulSoup

location_to_url_part = {
    'half_moon_bay': 'Half-Moon-Bay-California',
    'wrightsville_beach': 'Wrightsville-Beach-North-Carolina',
    'providence': 'Providence-Rhode-Island',
    'huntington_beach': 'Huntington-Beach',
}
location_to_full_name = {
    'half_moon_bay': 'Half Moon Bay, CA',
    'wrightsville_beach': 'Wrightsville Beach, NC',
    'providence': 'Providence, RI',
    'huntington_beach': 'Huntington Beach, CA'
}

base_url = 'https://www.tide-forecast.com/locations/{location}/tides/latest'

def get_daylight_tides_from_html(html):
    soup = BeautifulSoup(html, features='html.parser')
    today = soup.find('div', {'class': 'tide-header__today'})

    # Get date, sunrise, sunset
    paragraph = today.find('div', {'class': 'tide-paragraph'}).text
    date = re.search(r'tide times today on \w+ ([0-9]{1,2} [\w]* [0-9]{4})', paragraph, re.IGNORECASE).groups()[0]
    sunrise = re.search(r'Sunrise is at\s*([0-9]{1,2}:[0-9]{2}(am|pm))', paragraph, re.IGNORECASE).groups()[0]
    sunset = re.search(r'Sunset is at\s*([0-9]{1,2}:[0-9]{2}(am|pm))', paragraph, re.IGNORECASE).groups()[0]
    sunrise = datetime.datetime.strptime(date + ' ' + sunrise, '%d %B %Y %I:%M%p')
    sunset = datetime.datetime.strptime(date + ' ' + sunset, '%d %B %Y %I:%M%p')

    # get tide rows
    header, *rows = today.findAll('tr')
    headers = header.findAll('th')
    tideIndex = next(i for i, td in enumerate(headers) if td.text.startswith('Tide'))
    timeIndex = next(i for i, td in enumerate(headers) if td.text.startswith('Time'))
    heightIndex = next(i for i, td in enumerate(headers) if td.text.startswith('Height'))

    tides = []
    for row in rows:
        tide = dict()
        tds = row.findAll('td')
        kind = tds[tideIndex].b.contents[0]
        tide['type'] = 'low' if kind == 'Low Tide' else 'high'
        if tide['type'] != 'low':
            continue
        time = tds[timeIndex].b.contents[0].strip()
        tide['time'] = datetime.datetime.strptime(date + ' ' + time, '%d %B %Y %I:%M%p')
        if not sunrise < tide['time'] < sunset: # not daylight
            continue
        height = tds[heightIndex].b.contents[0].strip()
        tide['height'] = float(height.split()[0])
        kind = tds[tideIndex].b.contents[0]
        tide['type'] = 'low' if kind == 'Low Tide' else 'high'
        yield tide

def get_tides_for_location(location):
    url = base_url.format(location = location_to_url_part[location])
    html = requests.get(url).text
    for result in get_daylight_tides_from_html(html):
        result['location'] = location
        yield result

def get_tides_for_all_locations():
    for location in location_to_url_part:
        for result in get_tides_for_location(location):
            yield result


if __name__ == '__main__':
    for result in get_tides_for_all_locations():
        print("{}\t{:%I:%M%p}\t{} ft".format(location_to_full_name[result['location']], result['time'], result['height']))

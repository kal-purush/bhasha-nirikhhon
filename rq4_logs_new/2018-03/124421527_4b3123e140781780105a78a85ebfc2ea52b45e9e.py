#!/usr/bin/python
"""
Read the Met Office 3 hourly forecast for the location ID as the first
argument to the program. The forecasts for period 4 (9am-12pm) and 5 (12pm-3pm)
are pushed to the users phone using Pushbullet (weather type, temperature, wind
speed) as a sparkline.

This is inspired by https://github.com/met-office-lab/WeatherSpark_

If the weather for the periods above are good + the previous 3 periods
(9 hours) weather was ok, a smiley face/neutral face/sad face is also returned
indicating in one line the likelihood that the weather is good for tennis - see
function 'calc_nice_day_emoticon'

The pushbullet and met office API keys PUSHBULLET_API_KEY and
MET_OFFICE_API_KEY are set from environment variables
"""
from pushbullet import Pushbullet
import os
import sys
import requests
import datetime
import logging
"""Pushbullet and Met Office API keys are set from environment variables
"""
PUSHBULLET_API_KEY = os.environ['PUSHBULLET_API_KEY']
MET_OFFICE_API_KEY = os.environ['MET_OFFICE_API_KEY']
"""Fullwidth chars unicode characters:
http://www.fileformat.info/info/unicode/block/halfwidth_and_fullwidth_forms/
"""
FULLWIDTH_UNICODE = {' ': u"\u3000",
                     '-': u"\uFF0D",
                     '0': u"\uFF10",
                     '1': u"\uFF11",
                     '2': u"\uFF12",
                     '3': u"\uFF13",
                     '4': u"\uFF14",
                     '5': u"\uFF15",
                     '6': u"\uFF16",
                     '7': u"\uFF17",
                     '8': u"\uFF18",
                     '9': u"\uFF19",
                     'A': u"\uFF21",
                     'B': u"\uFF22",
                     'C': u"\uFF23",
                     'D': u"\uFF24",
                     'E': u"\uFF25",
                     'F': u"\uFF26",
                     'G': u"\uFF27",
                     'H': u"\uFF28",
                     'I': u"\uFF29",
                     'J': u"\uFF2A",
                     'K': u"\uFF2B",
                     'L': u"\uFF2C",
                     'M': u"\uFF2D",
                     'N': u"\uFF2E",
                     'O': u"\uFF2F",
                     'P': u"\uFF30",
                     'Q': u"\uFF31",
                     'R': u"\uFF32",
                     'S': u"\uFF33",
                     'T': u"\uFF34",
                     'U': u"\uFF35",
                     'V': u"\uFF36",
                     'W': u"\uFF37",
                     'X': u"\uFF38",
                     'Y': u"\uFF39",
                     'Z': u"\uFF3A",
                     'a': u"\uFF41",
                     'b': u"\uFF42",
                     'c': u"\uFF43",
                     'd': u"\uFF44",
                     'e': u"\uFF45",
                     'f': u"\uFF46",
                     'g': u"\uFF47",
                     'h': u"\uFF48",
                     'i': u"\uFF49",
                     'j': u"\uFF4A",
                     'k': u"\uFF4B",
                     'l': u"\uFF4C",
                     'm': u"\uFF4D",
                     'n': u"\uFF4E",
                     'o': u"\uFF4F",
                     'p': u"\uFF50",
                     'q': u"\uFF51",
                     'r': u"\uFF52",
                     's': u"\uFF53",
                     't': u"\uFF54",
                     'u': u"\uFF55",
                     'v': u"\uFF56",
                     'w': u"\uFF57",
                     'x': u"\uFF58",
                     'y': u"\uFF59",
                     'z': u"\uFF5A"}
"""Emoticons - unicode smiley faces used for good weather
http://www.fileformat.info/info/unicode/block/emoticons/list.htm
"""
EMOTICONS_UNICODE = {'SMILEY FACE WITH OPEN MOUTH': u"\U0001F603",
                     'NEUTRAL FACE':                u"\U0001F610",
                     'DISAPPOINTED FACE':           u"\U0001F61E",
                     'LOUDLY CRYING FACE':          u"\U0001F62D"}
"""Arrows - unicode single, double and triple arrows used for wind speed
http://www.fileformat.info/info/unicode/block/arrows/utf8test.htm
"""
ARROWS_UNICODE = {'RIGHTWARDS ARROW':         u"\u2192",
                  'RIGHTWARDS PAIRED ARROWS': u"\u21C9",
                  'THREE RIGHTWARDS ARROWS':  u"\u21F6"}
"""Degrees Celcius - unicode degrees celcius symbol
http://www.fileformat.info/info/unicode/char/2103/index.htm
"""
DEGREE_CELSIUS_UNICODE = u"\u2103"
"""Weather unicode symbols
https://www.fileformat.info/info/unicode/block/miscellaneous_symbols/list.htm
"""
WEATHER_UNICODE = {'CRESCENT MOON':            u"\U0001F319",
                   'BLACK SUN WITH RAYS':      u"\u2600",
                   'SUN BEHIND CLOUD':         u"\u26c5",
                   'CLOUD':                    u"\u2601",
                   'FOGGY':                    u"\U0001F301",
                   'UMBRELLA WITH RAIN DROPS': u"\u2614",
                   'SNOWFLAKE':                u"\u2744",
                   'THUNDER CLOUD AND RAIN':   u"\u26c8"}
"""A.M or P.M:
http://www.fileformat.info/info/unicode/block/cjk_compatibility/list.htm
"""
MERIDIEM_UNICODE = {'SQUARE AM': u"\u33C2",
                    'SQUARE PM': u"\u33D8"}
"""Met office weather types
https://www.metoffice.gov.uk/datapoint/support/documentation/code-definitions
"""
WEATHER_TYPES = {'Clear night':                 0,
                 'Sunny day':                   1,
                 'Partly cloudy (night)':       2,
                 'Partly cloudy (day)':         3,
                 'Not used':                    4,
                 'Mist':                        5,
                 'Fog':                         6,
                 'Cloudy':                      7,
                 'Overcast':                    8,
                 'Light rain shower (night)':   9,
                 'Light rain shower (day)':    10,
                 'Drizzle':                    11,
                 'Light rain':                 12,
                 'Heavy rain shower (night)':  13,
                 'Heavy rain shower (day)':    14,
                 'Heavy rain':                 15,
                 'Sleet shower (night)':       16,
                 'Sleet shower (day)':         17,
                 'Sleet':                      18,
                 'Hail shower (night)':        19,
                 'Hail shower (day)':          20,
                 'Hail':                       21,
                 'Light snow shower (night)':  22,
                 'Light snow shower (day)':    23,
                 'Light snow':                 24,
                 'Heavy snow shower: (night)': 25,
                 'Heavy snow shower (day)':    26,
                 'Heavy snow':                 27,
                 'Thunder shower (night)':     28,
                 'Thunder shower (day)':       29,
                 'Thunder':                    30}


def setup_logging():
    """Setup logging to console only at INFO level"""
    # create console handler with a higher log level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    # stringStream = io.StringIO()
    # stringHandler = logging.StreamHandler(stringStream)
    # stringHandler.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG,
                        format=' %(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%d/%m/%Y %I:%M:%S %p',
                        handlers=[console_handler])


def string_to_fullwidth(string, rjustlen=0):
    """Translate any string to fullwidth UNICODE characters."""
    fullwidth_string = ''
    if rjustlen > 0:
        string = string.rjust(rjustlen)
    for char in string:
        fullwidth_string += FULLWIDTH_UNICODE[char]
    return fullwidth_string


def get_am_pm(time):
    """Get the a.m. or p.m. unicode symbol depending on the time"""
    if time.hour < 12:
        return MERIDIEM_UNICODE['SQUARE AM']
    else:
        return MERIDIEM_UNICODE['SQUARE PM']


def get_weather_icon(weather_type):
    """Using the Met Office weather type, get the relevant emoji"""
    clear_night = [WEATHER_TYPES['Clear night']]
    sun = [WEATHER_TYPES['Sunny day']]
    sun_cloud = [WEATHER_TYPES['Partly cloudy (day)']]
    cloud = [WEATHER_TYPES['Partly cloudy (night)'],
             WEATHER_TYPES['Cloudy'],
             WEATHER_TYPES['Overcast']]
    fog = [WEATHER_TYPES['Mist'],
           WEATHER_TYPES['Fog']]
    rain = range(WEATHER_TYPES['Light rain shower (night)'],
                 WEATHER_TYPES['Hail'])
    snow = range(WEATHER_TYPES['Light snow shower (night)'],
                 WEATHER_TYPES['Heavy snow'])
    thunder = range(WEATHER_TYPES['Thunder shower (night)'],
                    WEATHER_TYPES['Thunder'])
    if weather_type in clear_night:
        return WEATHER_UNICODE['CRESCENT MOON']
    elif weather_type in sun:
        return WEATHER_UNICODE['BLACK SUN WITH RAYS']
    elif weather_type in sun_cloud:
        return WEATHER_UNICODE['SUN BEHIND CLOUD']
    elif weather_type in cloud:
        return WEATHER_UNICODE['CLOUD']
    elif weather_type in fog:
        return WEATHER_UNICODE['FOGGY']
    elif weather_type in rain:
        return WEATHER_UNICODE['UMBRELLA WITH RAIN DROPS']
    elif weather_type in snow:
        return WEATHER_UNICODE['SNOWFLAKE']
    elif weather_type in thunder:
        return WEATHER_UNICODE['THUNDER CLOUD AND RAIN']
    else:
        return WEATHER_UNICODE['CLOUD']


def get_temperature(temperature):
    """Right justify the temperature and add the degress centigrade symbol"""
    return "{}{}".format(string_to_fullwidth(str(temperature), 3),
                         DEGREE_CELSIUS_UNICODE)


def get_wind_speed(speed):
    """From the wind speed calculate the number of arrows to indicate the
    wind strength and then add the actual speed and mph
    """
    if speed < 10:
        wind_speed = ARROWS_UNICODE['RIGHTWARDS ARROW']
    elif speed < 20:
        wind_speed = ARROWS_UNICODE['RIGHTWARDS PAIRED ARROWS']
    else:
        wind_speed = ARROWS_UNICODE['THREE RIGHTWARDS ARROWS']
    wind_speed += string_to_fullwidth(str(speed), 2) + 'mph'
    return wind_speed


def calc_nice_day_emoticon(current_index, forecasts):
    """Using the current index in the forecasts list, look back at that and
    the previous three forecasts (so a total of 12 hours) to see if the weather
    was good (no rain, low wind)
    """
    nice_weather_types = [WEATHER_TYPES['Clear night'],
                          WEATHER_TYPES['Sunny day'],
                          WEATHER_TYPES['Partly cloudy (night)'],
                          WEATHER_TYPES['Partly cloudy (day)'],
                          WEATHER_TYPES['Cloudy']]
    recent_forecasts = forecasts[max(current_index - 3, 0):current_index + 1]
    for i, forecast in enumerate(reversed(recent_forecasts)):
        # Nice weather is a clear or cloudy day with a windspeed <= 15 mph
        # and change of rain <= 20%
        if not (forecast['weather_type'] in nice_weather_types and
                forecast['wind_speed_mph'] <= 15 and
                forecast['rain_probability_pc'] <= 20):
            if i <= 1:
                return EMOTICONS_UNICODE['LOUDLY CRYING FACE']
            else:
                logging.info('Might be ok?')
                return EMOTICONS_UNICODE['NEUTRAL FACE']
    logging.info('Nice day/time found!')
    return EMOTICONS_UNICODE['SMILEY FACE WITH OPEN MOUTH']


def get_met_office_3hourly_forecast(location_id):
    """Get the Met Office 3 hourly forecast using the location ID"""
    logging.info('Reading 3 hourly weather forecast for location ID {}'
                 .format(location_id))
    url = ('http://datapoint.metoffice.gov.uk/' +
           'public/data/val/wxfcs/all/json/' +
           location_id)
    response = requests.get(url, params={'res': '3hourly',
                                         'key': MET_OFFICE_API_KEY})
    response.raise_for_status()
    json_response = response.json()
    location_name = json_response['SiteRep']['DV']['Location']['name']
    forecasts = []
    for period in json_response['SiteRep']['DV']['Location']['Period']:
        period_date = datetime.datetime.strptime(period['value'], '%Y-%m-%dZ')
        for rep in period['Rep']:
            forecast = dict()
            forecast['period'] = int(rep['$']) / (24 * 60 / 8) + 1
            forecast['from'] = (period_date +
                                datetime.timedelta(minutes=int(rep['$'])))
            forecast['to'] = (forecast['from'] +
                              datetime.timedelta(minutes=(24 * 60 / 8),
                                                 seconds=-1))
            forecast['weather_type'] = int(rep['W'])
            forecast['temperature_c'] = int(rep['T'])
            forecast['wind_speed_mph'] = int(rep['S'])
            forecast['rain_probability_pc'] = int(rep['Pp'])
            forecasts.append(forecast)
    logging.info('{} forecasts read over {} days for {} from {} to {}'
                 .format(len(forecasts),
                         len(json_response['SiteRep']['DV']['Location']
                                          ['Period']),
                         location_name,
                         forecasts[0]['from'].strftime('%d/%m/%Y %H:%M'),
                         forecasts[-1]['to'].strftime('%d/%m/%Y %H:%M')))
    return location_name, forecasts


def main():
    setup_logging()
    # The first (and only) argument should be the location ID as found here:
    # http://datapoint.metoffice.gov.uk/public/data/val/wxfcs/all/xml/sitelist
    # ?res=daily&key=<MET OFFICE API KEY>
    if len(sys.argv) != 2:
        logging.error('Please enter a Met Office location ID as the first'
                      'argument')
        return
    location_name, forecasts = get_met_office_3hourly_forecast(sys.argv[1])
    try:
        body = ''
        for i, forecast in enumerate(forecasts):
            # Only send a forecast for periods 4 (9am-12pm) and 5 (12pm-3pm)
            if forecast['period'] == 4 or forecast['period'] == 5:
                logging.info('Calculating new forecast for {}'
                             .format(forecast['from'].
                                     strftime('%a %d/%m/%Y %H:%M')))
                if body != '':
                    body += '\n'
                # Day of week (MON, TUE, WED etc)
                body += string_to_fullwidth(forecast['from']
                                            .strftime('%a').upper())
                # am or pm
                body += get_am_pm(forecast['from'].time())
                # Nice day emoticon
                body += calc_nice_day_emoticon(i, forecasts)
                # weather icon
                body += get_weather_icon(forecast['weather_type'])
                # temprature in C
                body += get_temperature(forecast['temperature_c'])
                # wind speed in mph
                body += get_wind_speed(forecast['wind_speed_mph'])
        # Send this message to pushbullet
        pb = Pushbullet(PUSHBULLET_API_KEY)
        pb.push_note('Weather for {}'.format(location_name), body)
        logging.info('Forecast sent to pushbullet')
    except:
        logging.exception('Error occurred:')


if __name__ == "__main__":
    main()
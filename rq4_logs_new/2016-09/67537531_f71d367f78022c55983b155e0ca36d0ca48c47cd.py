#!/usr/bin/env python

import json
import urllib.request
from bs4 import BeautifulSoup
from time import strftime
import re
import datetime

from . import key
from . import logic

root_path = 'C:/Users/Eric/Desktop/Weatherman/'
api_arch_path = root_path + 'API_Arch/'
html_arch_path = root_path + 'HTML_Arch/'
screen_data_path = root_path + 'Screen_Data/'
actual_arch_path = root_path + 'ACT_Arch/'


def get_api_data(api_key):
    """API data gatherer.

    The api key is hidden.
    """
    req = urllib.request.Request(
        'http://api.openweathermap.org/data/2.5/forecast/city?' + api_key)
    with urllib.request.urlopen(req) as f:
        file_contents = f.read()
    return file_contents.decode('utf-8')


def store_api_file(contents, today_str):
    """API file storer.

    The file is stored directly in the Arch/ directory since it is being
      processed immediately and not being queued for processing.
    The file repo has each file with the date+time encoded in the filename.
    """
    with open((api_arch_path + today_str), 'w') as file:
        file.write(contents)


def get_data(source):
    """Generic data gatherer, for either HTML or JPG.

    The source is hidden.
    """
    with urllib.request.urlopen(source) as f:
        file_contents = f.read()
    return file_contents


def extract_fcst_soup(html_data):
    """Extract the forecast html soup item for subsequent searching.

    >>> from . import load_test_html
    >>> extract_fcst_soup(load_test_html.test_html)
    ... # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    [<div class="weather-box daily-forecast"><div
    class="weather-box-header">...<i class="fa
    fa-caret-right"></i></div></div></div></div>]
    """
    html_soup = BeautifulSoup(html_data, 'html.parser')
    return html_soup.find_all('div', class_='weather-box daily-forecast')


def store_jpg_file(contents, today_str):
    """JPG storing function.

    The file is stored directly in the Data/ directory since the processor
      is not implemented yet.
    The file repo has each file with the date+time encoded in the filename.
    Since these are jpg files, they are stored as bytes.
    """
    file_name = screen_data_path + 'screen_' + today_str + '.jpg'
    with open(file_name, 'wb') as file:
        file.write(contents)


def store_html_file(fcast_soup, today_str):
    """HTML storing function.

    The file is stored directly in the Arch/ directory since it is being
      processed immediately and not being queued for processing.
    Since the raw html files are big, this gets stripped down to just the
      forecast portion of the html.
    The file repo has each file with the date+time encoded in the filename.
    """
    fcast_html_string = str(fcast_soup)
    file_name = html_arch_path + '/html_' + today_str + '.html'
    with open(file_name, 'w') as file:
        file.write(fcast_html_string)


def process_api_data(json_string, today_str):
    """Main function to extract max and min temperatures from one API(json) file
        and save the contents to a DayRecord.

    The file contains temperature predictions every hour (not max/min) so the
       process is to find the max and min for a given day.
    The prediction date comes from when the file was read, and is encoded in
       the filename.  The json file does not contain prediction time.
    The date the forecast applies to is normalized to PDT, and max/min temps
       are found for the time from midnight to midnight.

    >>> from . import load_test_json
    >>> from . import models
    >>> today_str = '2016_06_16'
    >>> process_api_data(load_test_json.test_json, today_str)
    >>> forecasts = models.DayRecord.objects.all()
    >>> for forecast in forecasts:
    ...   print(str(forecast))
    2016-06-16, 0, api, 59, 51
    2016-06-17, 1, api, 63, 47
    2016-06-18, 2, api, 60, 47
    2016-06-19, 3, api, 69, 39
    2016-06-20, 4, api, 82, 44
    2016-06-21, 5, api, 82, 51
    """
    today_date = logic.get_date(today_str)
    json_data = json.loads(json_string)
    days_to_max_min = logic.get_retimed_fcsts_from_json(json_data, today_date)
    logic.process_days_to_max_min(days_to_max_min, today_date, 'api')


def process_html_data(daily_forecasts, today_str):
    """Main function to extract max and min temperatures from one HTML file
        and save the contents to a DayRecord.

    The prediction date comes from when the file was read, and is encoded in
       the filename.  The html file contains a forecast time; this is ignored.
    The date the forecast applies to is normalized to PDT, and max/min temps
       are found for the time from midnight to midnight.

    >>> from . import load_test_html
    >>> from . import models
    >>> today_str = '2016_07_24'
    >>> html_soup = extract_fcst_soup(load_test_html.test_html)
    >>> process_html_data(html_soup, today_str)
    >>> for forecast in models.DayRecord.objects.all():
    ...   print(str(forecast))
    2016-07-24, 0, html, 83, 59
    2016-07-25, 1, html, 82, 61
    2016-07-26, 2, html, 80, 62
    2016-07-27, 3, html, 84, 60
    2016-07-28, 4, html, 87, 62
    2016-07-29, 5, html, 92, 62
    """
    today_date = logic.get_date(today_str)
    days_to_max_min = logic.get_retimed_fcsts_from_html(daily_forecasts,
                                                        today_date)
    logic.process_days_to_max_min(days_to_max_min, today_date, 'html')


def extract_meas_soup(html_data):
    """Extract the forecast html soup item for subsequent searching.

    >>> from . import load_test_meas
    >>> extract_meas_soup(load_test_meas.test_meas_html)
    ... # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    [<div id="content"><h1>Yesterday's Weather for <span
    class="h1-blue">Portland, OR</span>...</strong></p><div
    style="clear:left"></div><div id="google_translate_element"></div></div>]
    """
    meas_soup = BeautifulSoup(html_data, 'html.parser')
    return meas_soup.find_all('div', id='content')


def store_meas_file(meas_soup, today_str):
    """Measured data HTML storing function.

    The file is stored directly in the Arch/ directory since it is being
      processed immediately and not being queued for processing.
    Since the raw html files are big, this gets stripped down to just the
      contents portion of the html.
    The file repo has each file with the date+time encoded in the filename.
    """
    meas_html_string = str(meas_soup)
    file_name = actual_arch_path + '/meas_' + today_str + '.html'
    with open(file_name, 'w') as file:
        file.write(meas_html_string)


def process_meas_data(daily_meas_soup, today_str):
    """Extract max and min temperatures from one HTML file and save the
         contents to an ActualDayRecord.

    The date the forecast applies to is normalized to PDT, and max/min temps
       are found for the time from midnight to midnight.

    >>> from . import load_test_meas
    >>> from . import models
    >>> today_str = '2016_07_24'
    >>> meas_soup = extract_meas_soup(load_test_meas.test_meas_html)
    >>> process_meas_data(meas_soup, today_str)
    >>> for actual in models.ActualDayRecord.objects.all():
    ...   print(str(actual))
    2016-07-23, PDX, 67, 51
    """
    location = 'PDX'
    max_re = re.compile('Yesterday\'s High</strong> was (-?\d+) ')
    min_re = re.compile('Yesterday\'s Low</strong> was (-?\d+) ')
    daily_meas = str(daily_meas_soup)
    max_temp = int(max_re.search(daily_meas).group(1))
    min_temp = int(min_re.search(daily_meas).group(1))
    meas_date = logic.get_date(today_str) - datetime.timedelta(1)
    try:
        act_temp_model = logic.get_actual(meas_date, location,
                                          max_temp, min_temp)
    except ValueError as error:
        print(error)
    else:
        act_temp_model.save()


def update_html_data():
    """Main function to update and archive the web-site based forecasts."""
    today_str = strftime('%Y_%m_%d')
    html_data = get_data(key.src2_str)
    html_soup = extract_fcst_soup(html_data)
    store_html_file(html_soup, today_str)
    process_html_data(html_soup, today_str)


def update_api_data():
    """Main function to update and archive the api based forecasts."""
    today_str = strftime('%Y_%m_%d')
    api_string = get_api_data(key.app_id_str)
    store_api_file(api_string, today_str)
    process_api_data(api_string, today_str)


def update_meas_data():
    """Main function to update and archive the measured temps."""
    today_str = strftime('%Y_%m_%d')
    meas_data = get_data(key.meas_str)
    meas_soup = extract_meas_soup(meas_data)
    store_meas_file(meas_soup, today_str)
    process_meas_data(meas_soup, today_str)


def main():
    update_html_data()
    update_api_data()
    update_meas_data()


if __name__ == '__main__()':
    main()
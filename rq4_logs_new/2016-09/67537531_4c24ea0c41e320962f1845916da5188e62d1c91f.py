#!/usr/bin/env python

from os import listdir, rename
from os.path import getsize, isfile, join
from . import logic
from . import models
from bs4 import BeautifulSoup
import json
import csv
import datetime
import re


root_path = 'C:/Users/Eric/Desktop/Weatherman/'
container_path = root_path + 'Reduced_Data/'
api_data_path = root_path + 'API_Data/'
api_arch_path = root_path + 'API_Arch/'
html_data_path = root_path + 'HTML_Data/'
html_arch_path = root_path + 'HTML_Arch/'
actual_data_path = root_path + 'ACT_Data/'
actual_arch_path = root_path + 'ACT_Arch/'
date_re = re.compile('20\d{2}_\d{2}_\d{2}')


def _get_html_soup(file_name):
    """HTML file loader."""
    with open(file_name) as html_file:
        html_string = html_file.read()
    return BeautifulSoup(html_string, 'html.parser')


def get_forecasts_from_html(html_soup):
    """HTML file content loader."""
    return html_soup.find_all('div', class_='weather-box daily-forecast')


def process_html_file(file_name):
    """Main function to extract max and min temperatures from one HTML file
        and save the contents to a DayRecord.

    The prediction date comes from when the file was read, and is encoded in
       the filename.  The html file contains a forecast time; this is ignored.
    The date the forecast applies to is normalized to PDT, and max/min temps
       are found for the time from midnight to midnight.
    """
    predict_date = logic.get_date(date_re.search(file_name).group(0))
    html_soup = _get_html_soup(file_name)
    daily_forecasts = get_forecasts_from_html(html_soup)
    days_to_max_min = logic.get_retimed_fcsts_from_html(daily_forecasts,
                                                        predict_date)
    logic.process_days_to_max_min(days_to_max_min, predict_date, 'html')


def _get_json(file_name):
    """JSON file content loader. """
    with open(file_name) as data_file:
        json_data = json.load(data_file)
    return json_data


def process_json_file(file_name):
    """Main function to extract max and min temperatures from one API(json) file
        and save the contents to a DayRecord.

    The file contains temperature predictions every hour (not max/min) so the
       process is to find the max and min for a given day.
    The prediction date comes from when the file was read, and is encoded in
       the filename.  The json file does not contain prediction time.
    The date the forecast applies to is normalized to PDT, and max/min temps
       are found for the time from midnight to midnight.
    """
    predict_date = logic.get_date(date_re.search(file_name).group(0))
    json_data = _get_json(file_name)
    days_to_max_min = logic.get_retimed_fcsts_from_json(json_data, predict_date)
    logic.process_days_to_max_min(days_to_max_min, predict_date, 'api')


def _process_csv_row(row, max_temp_index, min_temp_index):
    """Convert the csv rows into saved model records."""
    date = datetime.datetime.strptime(row[2], '%Y%m%d').date()
    location = models.LOCATIONS[row[1]]
    max_temp = int(row[max_temp_index])
    min_temp = int(row[min_temp_index])
    try:
        act_temp_model = logic.get_actual(date, location, max_temp, min_temp)
    except ValueError as error:
        print(error)
    else:
        act_temp_model.save()


def process_actual_file(filename):
    """Main function to extract actual temperatures from the .csv file and
       save the contents in an ActualDayRecord.
    TMAX/TMIN column parameterized in case the .csv file changes format.
    """
    with open(filename, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        header_row = next(csv_reader)
        max_temp_index = header_row.index('TMAX')
        min_temp_index = header_row.index('TMIN')
        for row in csv_reader:
            print(row)
            if row[1] == 'PORTLAND INTERNATIONAL AIRPORT OR US':
                _process_csv_row(row, max_temp_index, min_temp_index)


def move_file_to_archive(f, data_path, arch_path):
    """Move the processed file to the archive, by renaming it."""
    rename(data_path + f, arch_path + f)


def process_api_files():
    """Process the files loaded thought the API (JSON files).

    Comparison with 10kB done to strip off partial files (normal: 15kB)
    """
    only_files = [f for f in listdir(api_data_path)
                  if isfile(join(api_data_path, f))]
    for f in only_files:
        date_string = f[:10]
        print('processing API {}'.format(date_string))
        if getsize(api_data_path + f) > 10000:
            process_json_file(api_data_path + f)
            move_file_to_archive(f, api_data_path, api_arch_path)


def process_html_files():
    """Process the files loaded thought the web site (i.e., HTML).

    Comparison with 100KB done to strip off partial files (normal: 115kB)
    """
    only_files = [f for f in listdir(html_data_path)
                  if isfile(join(html_data_path, f))]
    for f in only_files:
        f_string = f[5:15]
        print('processing HTML {}'.format(f_string))
        if getsize(html_data_path + f) > 100000:
            process_html_file(html_data_path + f)
            move_file_to_archive(f, html_data_path, html_arch_path)


def process_actual_files():
    """Process the files loaded thought the web site as JPG."""
    only_files = [f for f in listdir(actual_data_path)
                  if isfile(join(actual_data_path, f))]
    for f in only_files:
        f_string = f
        print('processing Actuals {}'.format(f_string))
        if getsize(actual_data_path + f) > 10:
            process_actual_file(actual_data_path + f)
            move_file_to_archive(f, actual_data_path, actual_arch_path)


def main():
    process_html_files()
    process_api_files()
    process_actual_files()

if __name__ == '__main__':
    main()
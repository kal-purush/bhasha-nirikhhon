#!/usr/bin/env python

import urllib.request
from time import strftime
from . import key


def get_data():
    """Data harvesting module.

    Currently grabbing from three different sources, which come in three
      different formats.
    The JPG and HTML sources are hidden, along with the API key.
    The file repo has each file with the date+time encoded in the filename.
    """
    file_date = strftime('%Y_%m_%d_%H_%M_%S')
    fileRepo = 'C:/Users/Eric/Desktop/WeatherMan/'

    # API (JSON) source:
    req = urllib.request.Request('http://api.openweathermap.org/data/2.5/forecast/city?' +
                      key.app_id_str)
    with urllib.request.urlopen(req) as f:
        report = f.read()
    with open((fileRepo + 'Data/' + file_date), 'w') as file:
        file.write(report.decode('utf-8'))

    # JPG source:
    file_name = fileRepo + 'ScreenData/screen' + file_date + '.jpg'
    # urllib.request.urlretrieve(key.src1_str, fileName)
    with urllib.request.urlopen(key.src1_str) as f:
        file_contents = f.read()
    with open(file_name, 'wb') as file:
        file.write(file_contents)

    # HTML source:
    file_name = fileRepo + 'HTML_Data/html_' + file_date + '.html'
    # urllib.request.urlretrieve(key.src2_str, fileName)
    with urllib.request.urlopen(key.src2_str) as f:
        file_contents = f.read()
    with open(file_name, 'w') as file:
        file.write(file_contents.decode('utf-8'))


def main():
    get_data()


if __name__ == '__main__()':
    main()
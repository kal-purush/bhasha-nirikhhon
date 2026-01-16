# -*- coding: utf-8 -*-
"""
Loads FMI weather data
"""

from bs4 import BeautifulSoup
import datetime
import requests
import pytz

class FmiWeather(object):
    """
    Loads FMI weather data. Usage:

    weather = FmiWeather()
    weather.download_content(url)
    pprint.pprint(weather.parse_next_days())
    """

    DIRECTIONS = {
        u"Koillis": "NE",
        u"Kaakkois": "SE",
        u"Lounais": "SW",
        u"Luoteis": "NW",
        u"Pohjois": "N",
        u"Etelä": "S",
        u"Länsi": "W",
        u"Itä": "E",
    }

    SIMPLE_OBSERVATION_PARSER = (
        # Keyname, unit, fieldname
        (u"Lämpötila", "C", "temperature"),
        (u"Kosteus", "%", "humidity"),
        (u"Kastepiste", "C", "dewpoint"),
        (u"Puuska", "m/s", "wind_gust"),
        (u"Paine", "hPa", "pressure"),
        (u"Lumensyvyys", "cm", "snow"),
    )


    @classmethod
    def _get_second(cls, value):
        """ Gets second value, separated by unicode non-line-breaking space """
        value = value.split(u"\xa0", 1)
        return value[1]


    @classmethod
    def _parse_float(cls, value):
        """ Parses float field (may contain unit) to python float. Trailing > is removed. """
        value = value.replace(",", ".").split(u"\xa0")
        value = value[0]
        value = value.split(" ")
        if value[0] == ">":
            value = value[1:]
        return float(value[0])


    @classmethod
    def _parse_rainfall(cls, item):
        """ Parses rainfall row item """
        item = item.find("strong")
        if item is None:
            raise ValueError("Rainfall item does not include value")
        return cls._parse_float(item.text)


    @classmethod
    def _parse_ppcp(cls, item):
        """ Parses precipitation probabilities row item """
        item = item.find("span")
        if item is None:
            raise ValueError("ppcp item does not include value")
        return cls._parse_float(item.text) / 100


    @classmethod
    def _parse_temperature(cls, item):
        """ Parses temperature row item """
        item = item.find("span")
        if item is None:
            raise ValueError("Temperature item does not include value")
        value = item.text.replace(u"\xb0", "")
        return cls._parse_float(value)


    @classmethod
    def _parse_symbol(cls, item):
        """ Parse symbol (icon) row item """
        item = item.find("div")
        if item is None:
            raise ValueError("Symbol does not have content")
        for cla in item["class"]:
            if cla.startswith("code-"):
                cla = cla.split("-")
                return "%s/%s" % (cla[1], cla[2])


    @classmethod
    def _parse_datetime(cls, text):
        """ Parses 23.3.2015 1:00" styled timestamps """
        if text[-1] == ".":
            text = text[:-1]
        date, time = text.split(u"\xa0")[0].split(" ")

        date = date.split(".")
        # Add leading zeros
        if len(date[1]) == 1:
            date[1] = "0" + date[1]
        if len(date[0]) == 1:
            date[0] = "0" + date[0]
        date = "%s-%s-%s" % (date[2], date[1], date[0]) # YYYY-MM-DD

        time = time.split(":")
        if len(time[0]) == 1:
            time[0] = "0" + time[0]
        time = "%s:%s" % (time[0], time[1])

        text = "%sT%s" % (date, time)
        parsed_timestamp = datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M")
        # All times are in Europe/Helsinki. Make datetime objects timezone aware,
        # so that this information is preserved.
        return pytz.timezone("Europe/Helsinki").localize(parsed_timestamp)


    @classmethod
    def _parse_wind(cls, item):
        """ Parses wind row item """
        item = item.find("div")
        if item is None:
            raise ValueError("Wind does not have content")
        for cla in item["class"]:
            if cla.startswith("code-"):
                cla = cla.split("-")
                return {"direction": cla[1], "speed": int(cla[2])}


    @classmethod
    def _process_row(cls, item, selector, callback):
        """ Process row - "temperatures", "wind information" etc. - for
            hourly or daily forecasts """
        row = item.find("tr", {"class": selector})
        items = []
        for row_item in row.find_all("td"):
            items.append(callback(row_item))
        return items


    def __init__(self):
        self.soup = None
        self.deterministic_timestamp = None
        self.probabilistic_timestamp = None


    def load_string(self, data):
        """ Loads HTML string """
        self.soup = BeautifulSoup(data)

        updated_timestamp = self.soup.find("div", {"class": "local-weather-forecast-metadata"})
        def get_timestamp(field):
            """ Parse modification timestamps """
            data = updated_timestamp.find("span", {"class": field}).text.rsplit(" ")
            return self._parse_datetime("%s %s" % (data[-2], data[-1]))
        self.deterministic_timestamp = get_timestamp("deterministic")
        self.probabilistic_timestamp = get_timestamp("probabilistic")


    def open_file(self, filename):
        """ Opens, reads and loads given filename """
        self.load_string(open(filename).read())


    def download_content(self, url):
        """ Downloads and loads given URL """
        response = requests.get(url)
        if response.status_code == 200:
            self.load_string(response.text)


    def _parse_tables(self, item):
        """ Parses day/hour forecast tables """
        content_table = item.find("table")

        timeinfo = []

        # Fetch datetime for each column
        hour_items = content_table.find("thead")
        hour_items = hour_items.find("tr", {"class": "meteogram-times"}).find_all("td")
        for hour_item in hour_items:
            timeinfo.append(self._parse_datetime(hour_item.find("span")["title"]))

        process_fields = (
            ("icon", "meteogram-weather-symbols", self._parse_symbol),
            ("temperature", "meteogram-temperatures", self._parse_temperature),
            ("wind", "meteogram-wind-symbols", self._parse_wind),
            ("feelslike", "meteogram-apparent-temperatures", self._parse_temperature),
            ("ppcp", "meteogram-probabilities-of-precipitation", self._parse_ppcp),
            ("rainfall", "meteogram-hourly-precipitation-values", self._parse_rainfall),
        )

        # Fetch raw information for each row
        data_raw = {}
        for (fieldname, selector, callback) in process_fields:
            data_raw[fieldname] = self._process_row(content_table, selector, callback)

        def combine_data():
            """ Combine raw data rows with timestamps """
            data = []
            for i, timestamp in enumerate(timeinfo):
                single_data = {
                    "timestamp": timestamp,
                }
                for field, item_data in data_raw.items():
                    single_data[field] = item_data[i]
                data.append(single_data)
            return data

        return {"meta": {"updated_at": self.deterministic_timestamp}, "forecast": combine_data()}


    def parse_next_days(self):
        """ Parses information for next 6 days """
        if self.soup is None:
            raise ValueError("No data loaded yet. Use open_file or download_content to fetch data")

        item = self.soup.find("div", {"class": ["mid", "local-weather-forecast"]})
        return self._parse_tables(item)


    def parse_next_hours(self):
        """ Parses information for next 18 hours """
        item = self.soup.find("div", {"class": ["short", "local-weather-forecast"]})
        return self._parse_tables(item)


    def parse_all_forecasts(self):
        """ Parses both hourly and daily forecasts and return combined information """

        items = self.parse_next_hours()
        existing_dates = set([obs["timestamp"] for obs in items["forecast"]])

        for item in self.parse_next_days()["forecast"]:
            if item["timestamp"] not in existing_dates:
                items.append(item)
            existing_dates.add(item["timestamp"])

        items["forecast"] = sorted(items["forecast"], key=lambda k: k["timestamp"])
        return items


    def parse_observations(self):
        """ Parses latest observations """

        item = self.soup.find("table", {"class": "observation-text"})
        parsed_timestamp = self._parse_datetime(item.find("span", {"class": "time-stamp"}).text)

        values = {"observed_at": parsed_timestamp}

        item = item.find("tbody")
        for data in item.find_all("span", {"class": "parameter-name-value"}):
            keyname = data.find("span", {"class": "parameter-name"}).text
            value = data.find("span", {"class": "parameter-value"}).text

            found = False
            for (key_candidate, unit, fieldname) in self.SIMPLE_OBSERVATION_PARSER:
                if keyname == key_candidate:
                    values[fieldname] = {"value": self._parse_float(value), "unit": unit}
                    found = True
                    break
            if found:
                continue

            if keyname.startswith(u"Näkyvyys"):
                values["visibility"] = {
                    "value": int(self._parse_float(value)),
                    "unit": "km"}
            elif keyname.startswith(u"Tunnin\xa0sadekertymä"):
                values["rainfall"] = {"value": self._parse_float(value), "unit": "mm"}
            elif value.endswith("m/s"):
                values["wind_speed"] = {"value": self._parse_float(value), "unit": "m/s"}
                for text, direction in self.DIRECTIONS.items():
                    if keyname.startswith(text):
                        values["wind_direction"] = {"value": direction, "unit": ""}
            elif value.startswith("(") and value.endswith(")") and "/" in value:
                parsed_value = int(value.split("/")[0].replace("(", "")) / 8.0 * 100
                values["cloudiness"] = {"value": parsed_value, "unit": "%"}
            else:
                print keyname, "--", value

        return values
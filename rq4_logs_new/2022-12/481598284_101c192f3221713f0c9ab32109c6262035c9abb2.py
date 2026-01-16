"""this module performs extract, transform, load (ETL) operations
for the sleep dataset
"""
import sys
import dataset as ds
import datetime as dt
import numpy as np
import pandas as pd


TABLE_SCHEMA = {
    'nights': {
        'date': dt.date,
        'start_hrs': float,
        'end_hrs': float,
        'duration_hrs': float,
        'year': int,
        'month': int,
        'week': int,
        'DOW': int
    }
}

def nights_update():
    """loads the recent events, transforms into nights, posts the nights database updates
    """
    #01 load events
    ds.update()

    #02 transform into nights
    events = ds.DATASETS['events']
    nights = nights_from_events(events)

    #03 update sql table
    ds.db.update_table(nights, 'nights', False)

    gs_nights = nights.copy()
    date_format = ds.db.GSHEET_CONFIG['sleep']['sheets']['nights']['date_format']
    time_format = ds.db.GSHEET_CONFIG['sleep']['sheets']['nights']['time_format']
    datetime_format = date_format + ' ' + time_format
    gs_nights['date'] = gs_nights['date'].apply(lambda x: x.strftime(datetime_format))

    #04 post to gsheet
    ds.db.post_to_gsheet(gs_nights, 'sleep', 'nights', input_option='USER_ENTERED')


def nights_from_events(events):
    # convert from str to datetime
    events['timestamp'] = events.apply(
        lambda x: dt.datetime.combine(
            x['date'],
            x['time'].to_pydatetime().time()
        ), axis=1)

    # shift by +8hrs to center middle of sleep near to middle of day
    events['timestamp_s'] = events['timestamp'].apply(
        lambda x: x + dt.timedelta(hours=8))
    events['event_end_s'] = events.apply(
        lambda x: x['timestamp_s'] + dt.timedelta(hours=x['duration_hrs']), axis=1)

    # add sleep event start times
    events['start_date_s'] = events['timestamp_s'].apply(lambda x: x.date())
    events['start_time_s'] = events['timestamp_s'].apply(lambda x: x.time())

    # add sleep event end times
    events['end_date_s'] = events['event_end_s'].apply(lambda x: x.date())
    events['end_time_s'] = events['event_end_s'].apply(lambda x: x.time())

    nights = pd.pivot_table(events, index='start_date_s', values=[
        'start_time_s', 'end_time_s', 'duration_hrs'],
                            aggfunc={'start_time_s': 'min',
                                     'end_time_s': 'max',
                                     'duration_hrs': np.sum})

    # shift back by 8 hrs
    def shift_time(raw, hrs):
        timestamp_raw = dt.datetime.combine(dt.date(2000, 1, 1), raw)
        shifted = (timestamp_raw + dt.timedelta(hours=hrs)).time()
        return shifted

    nights.reset_index(inplace=True)
    nights['date'] = nights['start_date_s'].apply(lambda x: x + dt.timedelta(days=-1))
    nights['time'] = nights['start_time_s'].apply(lambda x: shift_time(x, -8))
    nights['end_time'] = nights['end_time_s'].apply(lambda x: shift_time(x, -8))

    del nights['start_date_s'], nights['start_time_s'], nights['end_time_s']

    # update the year, month, week and DOW fields
    nights['year'] = nights['date'].apply(lambda x: x.year)
    nights['month'] = nights['date'].apply(lambda x: x.month)
    nights['week'] = nights['date'].apply(lambda x: x.isocalendar()[1])
    nights['DOW'] = nights['date'].apply(lambda x: x.weekday())
    nights['start_hrs'] = nights['time'].apply(
        lambda x: x.hour + x.minute / 60 + x.second / 3600)
    nights['end_hrs'] = nights['end_time'].apply(
        lambda x: x.hour + x.minute / 60 + x.second / 3600)
    nights['start_hrs'] = nights['start_hrs'].apply(
        lambda x: x if x > 12 else 24 + x)

    fields = [f for f in TABLE_SCHEMA['nights']]
    nights = nights[fields]
    return nights


if __name__ == '__main__':
    function_name = sys.argv[1]
    if function_name == 'nights_update':
        nights_update()
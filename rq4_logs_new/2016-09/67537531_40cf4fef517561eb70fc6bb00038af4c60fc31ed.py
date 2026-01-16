"""Weather Maniac Statistics modules.

These functions deal with:
  -- Calculating the statistics (mean, stdev)
  -- Creating the JSON string to be returned to the Web Site.
"""

import datetime
import math
import random
from django.db.models import Max, Min

from . import data_loader
from . import models
from . import utilities
from . import histogram


def get_statistics_per_day(bins):
    """Get the statistics from a collection of bins.

    >>> from . import load_test_records
    >>> load_test_records.histo_loader()
    >>> get_statistics_per_day([])
    (0, 0)
    >>> bins = histogram.get_all_bins('api', 'PDX', 'max', 2)
    >>> get_statistics_per_day(bins)
    (2.0, 0.6324555320336759)
    """
    total = sum([ebin.quantity for ebin in bins])
    if total == 0:    # Avoids a div by zero error; stats are nulled out.
        return 0, 0
    mean = sum([ebin.error * ebin.quantity for ebin in bins]) / total
    variance = sum([(ebin.error - mean)**2 * ebin.quantity for ebin in bins])
    std = math.sqrt(variance / (total - 1))
    return mean, std


def get_statistics(source, location, mtype):
    """Main function to collect statistics for application to the forecast
         points on the web-site.

    >>> from . import load_test_records
    >>> load_test_records.histo_loader()
    >>> get_statistics('api', 'PDX', 'max')
    ...   # doctest: +NORMALIZE_WHITESPACE
    ({0: 2.0, 1: 2.0, 2: 2.0, 3: 2.0, 4: 2.0},
    {0: 0.6324555320336759, 1: 0.6324555320336759, 2: 0.6324555320336759,
    3: 0.6324555320336759, 4: 0.6324555320336759})
    """
    means = {}
    stds = {}
    for day in range(models.SOURCE_TO_LENGTH[source]):
        latest_bin = histogram.get_latest_histogram_bin(source, location,
                                                        mtype, day)
        latest_day = histogram.get_latest_matching_day(source, location,
                                                       day, latest_bin)
        if latest_day > latest_bin:
            histogram.populate_histogram(source, location, mtype,
                                         day, latest_bin)
        bins = histogram.get_all_bins(source, location, mtype, day)
        means[day], stds[day] = get_statistics_per_day(bins)
    return means, stds


def obfuscate_forecast(forecast, start_date):
    """Obfuscate forecast.

    >>> forecast = {0: 50, 1: 51, 2: 52, 3: 53, 4: 54}
    >>> obfuscate_forecast(forecast, datetime.date(2016, 8, 20))
    {0: 50, 1: 52, 2: 52, 3: 54, 4: 53}
    """
    random.seed(a=str(start_date)[-1:])  # change rand num only when day changes
    for day, temp in forecast.items():
        forecast[day] = round(temp + random.uniform(-0.75, 0.75))
    return forecast


def make_json_of_forecast(forecast, means, stds, source, start_date):
    """Create the JSON object from the forecast and statistics data.

    >>> forecast = {0: 50, 1: 51, 2: 52, 3: 53, 4: 54}
    >>> means = {0: 1, 1: 0, 2: -1, 3: 0, 4: 1}
    >>> stds = {0: 1, 1: 1, 2: 1, 3: 1, 4: 1}
    >>> start_date = datetime.date(2016, 8, 1)
    >>> json = make_json_of_forecast(forecast, means, stds, 'api', start_date)
    >>> for item in json:
    ...   print(sorted(item.items()))
    ...   # doctest: +NORMALIZE_WHITESPACE
    [('date', '2016-08-01'), ('pct05', 47.04), ('pct25', 48.326), ('pct50', 49),
        ('pct75', 49.674), ('pct95', 50.96), ('source_raw', 50)]
    [('date', '2016-08-02'), ('pct05', 49.04), ('pct25', 50.326), ('pct50', 51),
        ('pct75', 51.674), ('pct95', 52.96), ('source_raw', 51)]
    [('date', '2016-08-03'), ('pct05', 51.04), ('pct25', 52.326), ('pct50', 53),
        ('pct75', 53.674), ('pct95', 54.96), ('source_raw', 52)]
    [('date', '2016-08-04'), ('pct05', 51.04), ('pct25', 52.326), ('pct50', 53),
        ('pct75', 53.674), ('pct95', 54.96), ('source_raw', 53)]
    [('date', '2016-08-05'), ('pct05', 51.04), ('pct25', 52.326), ('pct50', 53),
        ('pct75', 53.674), ('pct95', 54.96), ('source_raw', 54)]
    """
    json = []
    for ddate in range(models.SOURCE_TO_LENGTH[source]):
        if ddate in forecast:
            json.append({
                'date': str(start_date + datetime.timedelta(ddate))[:10],
                'source_raw': forecast[ddate],
                'pct05': forecast[ddate] - means[ddate] - 1.96 * stds[ddate],
                'pct25': forecast[ddate] - means[ddate] - 0.674 * stds[ddate],
                'pct50': forecast[ddate] - means[ddate],
                'pct75': forecast[ddate] - means[ddate] + 0.674 * stds[ddate],
                'pct95': forecast[ddate] - means[ddate] + 1.96 * stds[ddate]
            })
    return json


def return_json_of_forecast(source, mtype):
    """Main function to return a JSON object containing the dates, forecast temp
         points and the statistical spread.
    """
    location = 'PDX'
    start_date = datetime.date.today()
    forecast = data_loader.get_forecast(source, mtype, start_date)
    forecast = obfuscate_forecast(forecast, start_date)
    means, stds = get_statistics(source, location, mtype)
    json = make_json_of_forecast(forecast, means, stds, source, start_date)
    return json


def get_start_bin_date(ebins, start_date):
    """Return the earlier of start_date and what is in the bins.

    >>> from . import load_test_records
    >>> load_test_records.histo_loader()
    >>> ebins = histogram.get_all_bins('api', 'PDX', 'min', 2)
    >>> start_date = datetime.date(2016, 7, 1)
    >>> get_start_bin_date(ebins, start_date)
    datetime.date(2016, 6, 1)
    """
    bin_start = ebins.aggregate(Min('start_date'))['start_date__min']
    return min([bin_start, start_date])


def get_end_bin_date(ebins, end_date):
    """Return the later of end_date and what is in the bins.

    >>> from . import load_test_records
    >>> load_test_records.histo_loader()
    >>> ebins = histogram.get_all_bins('api', 'PDX', 'min', 2)
    >>> end_date = datetime.date(2016, 7, 1)
    >>> get_end_bin_date(ebins, end_date)
    datetime.date(2016, 8, 1)
    """
    bin_end = ebins.aggregate(Max('end_date'))['end_date__max']
    return max([bin_end, end_date])


def find_max_error(ebins):
    """Return the maximum error from the bins.

    >>> from . import load_test_records
    >>> load_test_records.histo_loader()
    >>> ebins = histogram.get_all_bins('api', 'PDX', 'min', 2)
    >>> find_max_error(ebins)
    3.0
    """
    max_pos_error = ebins.aggregate(Max('error'))['error__max']
    max_neg_error = ebins.aggregate(Min('error'))['error__min']
    return utilities.find_abs_largest([max_pos_error, max_neg_error])


def make_stats_json(source, mtype):
    """Get the stats JSON

    >>> from . import load_test_records
    >>> load_test_records.histo_loader()
    >>> json = make_stats_json('api', 'max')
    >>> sorted(json.items())
    ...   # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    [('end_date', datetime.date(2016, 8, 1)), ('mtype', 'max'),
    ('source', 'Service B'), ('start_date', datetime.date(2016, 6, 1)),
    ('stats_by_day', [...])]
    >>> for day in json['stats_by_day']:
    ...   sorted(day.items())
    [('day', 0), ('max', 3.0), ('mean', 2.0), ('std', 0.6324555320336759)]
    [('day', 1), ('max', 3.0), ('mean', 2.0), ('std', 0.6324555320336759)]
    [('day', 2), ('max', 3.0), ('mean', 2.0), ('std', 0.6324555320336759)]
    [('day', 3), ('max', 3.0), ('mean', 2.0), ('std', 0.6324555320336759)]
    [('day', 4), ('max', 3.0), ('mean', 2.0), ('std', 0.6324555320336759)]
    """
    end_date = datetime.date(2016, 5, 1)
    start_date = datetime.date(2116, 6, 1)
    stats_by_day = []
    mean, std = get_statistics(source, 'PDX', mtype)
    for day in range(models.SOURCE_TO_LENGTH[source]):
        ebins = histogram.get_all_bins(source, 'PDX', mtype, day)
        start_date = get_start_bin_date(ebins, start_date)
        end_date = get_end_bin_date(ebins, end_date)
        record_by_day = {
            'day': day,
            'mean': mean[day],
            'std': std[day],
            'max': find_max_error(ebins)
        }
        stats_by_day.append(record_by_day)
    return {
        'source': models.SOURCE_TO_NAME[source],
        'mtype': mtype,
        'stats_by_day': stats_by_day,
        'start_date': start_date,
        'end_date': end_date
        }


def main():
    pass

if __name__ == '__main__()':
    main()
from decimal import Decimal
from exceptions import CliUserError
from utils import parse_date, lookup_city, date_tostring

def max_temp_delta(
    records,
    city_shorthand,
    year=None,
    month=None
):
    # Validate month/year options
    if month is not None and year is None:
        raise CliUserError('Must also provide a year, if a month is provided')

    # Lookup the full city name
    city = lookup_city(city_shorthand)
    
    # track max temp change, and date
    max_delta = (0, None)

    for row in records:
        # Skip rows that aren't in the given city
        if row['NAME'] != city:
            continue
        
        # Skip records not in the provided year/month
        date = parse_date(row['DATE'])
        if (year is not None and date.year != year):
            continue
        if (month is not None and date.month != month):
            continue

        # Calculate temp delta, see if it's the new max
        #
        # Use decimal types to avoid floating point errors
        # https://docs.python.org/3/library/decimal.html#floating-point-notes
        temp_delta = Decimal(row['TMAX']) - Decimal(row['TMIN'])
        if temp_delta > max_delta[0]:
            max_delta = (temp_delta, date)

    return {
        'city': city_shorthand,
        'date': date_tostring(max_delta[1]),
        'temp_change': float(max_delta[0])
    }
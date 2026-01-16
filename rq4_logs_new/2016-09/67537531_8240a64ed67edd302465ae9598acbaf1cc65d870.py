"""Weather Maniac Analysis modules."""

from . import models

def display_error_vs_date(source, location, type, day_in_advance):
    """Test function to print error."""
    actuals = models.ActualDayRecord.objects.filter(location=location, type=type)
    for act_record in actuals:
        date = act_record.date_meas
        try:
            forecast = models.DayRecord.objects.get(
                date_reference=date,
                source=source,
                day_in_advance=day_in_advance,
            )
        except models.DayRecord.DoesNotExist:
            print('no matching record for {}'.format(date))
        else:
            meas = act_record.temp
            if type == 'max':
                fcst = forecast.max_temp
            else:
                fcst = forecast.min_temp
            error = fcst - meas
            print('Date: {}, Meas: {}, Fcst: {}, Error: {}'.format(
                date, meas, fcst, error
            ))
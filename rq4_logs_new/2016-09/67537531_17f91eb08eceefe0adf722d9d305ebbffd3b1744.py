"""Test record loader for tests."""

from . import models
import datetime

def test_loader():
    """This dumps data into histograms for testing purposes."""
    for source in models.SOURCES:
        for type in models.TYPES:
            for day in range(models.SOURCE_TO_LENGTH[source]):
                histo = models.ErrorHistogram(
                    source=source,
                    type=type,
                    location='PDX',
                    day_in_advance=day)
                histo.save()
                for error in range(1, 4):
                    qty = 10 - 7 * abs(error - 2)  # gives 3, 10, 3 distrib.
                    models.ErrorBin(
                        member_of_hist=histo,
                        error=error,
                        quantity=qty,
                        start_date=datetime.date(2016, 6, 1),
                        end_date=datetime.date(2016, 8, 1)
                    ).save()
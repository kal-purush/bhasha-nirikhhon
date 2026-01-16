from __future__ import print_function
import datetime

now = datetime.datetime.now()
dates = []
for d in range(7, 0, -1):
    dates.append(datetime.datetime.now() - datetime.timedelta(days=d))
for d in range(1, 8):
    dates.append(datetime.datetime.now() + datetime.timedelta(days=d))
for _date in dates:
    diff = _date - now
    print(_date.strftime('%Y%m%d'), diff.days)
    if diff.days == 3:
        print('Alert @ 3 days!')
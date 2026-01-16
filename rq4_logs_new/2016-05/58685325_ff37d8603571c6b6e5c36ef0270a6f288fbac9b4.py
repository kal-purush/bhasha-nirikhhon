import json
import datetime
import csv
import sys
import dateutil.parser as parser

python_version = sys.version_info.major
if python_version == 3:
    import configparser
else:
    import ConfigParser as configparser


def toDatetime(str):
    return parser.parse(str)


if __name__ == '__main__':
    config = configparser.RawConfigParser()
    config.read('../config.properties')
    datapath = config.get('Path', 'json_data')
    batchpath = config.get('Path', 'batch_unlabelled')

    account = ['unlabelled_SkyNewsBreak']

    begin = toDatetime('Sun May 22 00:00:00 +0000 2016')
    end = toDatetime('Sat May 29 23:59:59 +0000 2016')
    delta = datetime.timedelta(hours=12)
    times = []
    date = begin
    while date <= end:
        times.append(date)
        date = date + delta
    beforeTime = begin - delta

    files = []
    writers = []
    for date in times:
        file = open(batchpath + date.strftime("%Y%m%d_%H") + '.csv', 'w')
        files.append(file)
        writers.append(csv.writer(file, delimiter='`', quotechar='|', quoting=csv.QUOTE_ALL))

    for i in range(len(account)):
        acc = account[i]
        count = 0
        f = open(datapath + acc + '.json')
        for line in f:
            tweet = json.loads(line)
            time = toDatetime(tweet['created_at'])
            # can be optimized
            current = begin
            index = 0
            if begin <= time <= end and tweet['in_reply_to_status_id'] is None:
                while time >= current:
                    current = current + delta
                    index += 1
                row = [tweet['text'].encode('ascii', 'replace'),  acc]
                writers[index - 1].writerow(row)
        f.close()

    for file in files:
        file.close()

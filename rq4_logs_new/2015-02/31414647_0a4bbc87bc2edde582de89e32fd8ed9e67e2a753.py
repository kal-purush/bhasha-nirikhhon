# -*- coding: utf-8 -*-
import os, sys, re, csv
from operator import itemgetter
import simplejson
from pprint import pprint
import dateutil.parser

def parse_logs():
    FS_ENCODING = sys.getfilesystemencoding()
    THIS_FILE = __file__.decode(FS_ENCODING)
    BASE_DIR = os.path.dirname(os.path.realpath(os.path.abspath(THIS_FILE)))
    LOGS_DIR = os.path.join(BASE_DIR, 'raw_logs')
    
    # gets all log files
    files = [os.path.join(LOGS_DIR, f) for f in os.listdir(LOGS_DIR) if os.path.isfile(os.path.join(LOGS_DIR, f))]
    
    # retrieves thoses telemtry ones
    telemetry_files = []
    for log_file in files:
        if log_file[-9:] == 'telemetry':
            telemetry_files.append(log_file)

    # prepares CSV file
    csv_file = open(os.path.join(BASE_DIR, 'telemetry_logs.csv'), 'wb')
    writer = csv.writer(csv_file, delimiter=';', quoting=csv.QUOTE_NONE)
    headers = ['Horodatage', 'Application', 'Version', 'IP', 'ID Anonyme', 'Type', 'Trace', 'Extra']
    writer.writerow(headers)
    traces_count = 0
    users = []
    rows = []

    # parses telemetry files
    for log in telemetry_files:
        current_log = open(log, 'rb')
        content = current_log.readlines()
        for line in content:
            json_line = line.replace('\'','"')
            data = simplejson.loads(json_line)
            for trace in data['TelemetryDataLights']:
                row = []
                row.append(dateutil.parser.parse(trace['UtcTime']).strftime("%d-%m-%Y %H:%M:%S"))
                row.append(data['TelemetryDataAppInfo']['AppName'])
                row.append(data['TelemetryDataAppInfo']['AppVersion'])
                row.append(data['TelemetryDataAppInfo']['Ip'])
                row.append(data['TelemetryDataAppInfo']['LocalId'])
                row.append(trace['TelemetryDataType'])
                row.append(trace['Parameter'])
                if trace['Extra']:
                    row.append(trace['Extra'].encode('utf-8'))
                else:
                    row.append('')
                if data['TelemetryDataAppInfo']['LocalId'] not in users:
                    users.append(data['TelemetryDataAppInfo']['LocalId'])
                traces_count += 1
                rows.append(row)
        current_log.close()

    # finalizes CSV file
    rows = sorted(rows, key=itemgetter(0))
    writer.writerows(rows)
    writer.writerow(['',''])
    writer.writerow(['Nombre de traces', traces_count])
    writer.writerow([r'Nombre de users différents', len(users)])
    csv_file.close()


if __name__ == '__main__':
    parse_logs()
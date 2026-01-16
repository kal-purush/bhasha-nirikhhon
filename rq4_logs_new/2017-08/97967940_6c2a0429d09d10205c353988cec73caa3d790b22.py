from influxdb import InfluxDBClient
import time
import datetime


db = InfluxDBClient('localhost', 8086, 'root', 'root', 'TestDB')

db.create_database('TestDB')
retention_policy = 'server_data'
try:
    db.create_retention_policy('TestDB', '1d', 1, default=True)
except Exception: pass

result = []
for i in range(100):
    now = time.mktime(datetime.datetime.now().utctimetuple())
    pointValues = {
        'time': int(now)*1000000000+i,
        'measurement': 'domains',
        'tags': {
            'domain': 'test{}.cl'.format(i)
        },
        'fields': {
            'value': 1
        }
    }

    result.append(pointValues)

print db.write_points(result, retention_policy='TestDB')

# TODO: Usar esta consulta en graphite, grafana no quiere utilizar una columna como un resultado para hacer tag
# SELECT domain, top FROM (SELECT TOP(n, 2), domain FROM (SELECT sum("value") as n FROM "domains" WHERE time > now()-1h GROUP BY time(1m), domain) where time > now()-1h group by time(1m))

#SELECT TOP(n, 2), domain FROM (SELECT sum("value") as n FROM "domains" WHERE time > now()-1h GROUP BY time(1m), domain) where time > now()-1h group by time(1m)
#SELECT TOP(n,2), domain FROM (SELECT sum("value") as n FROM "domains" WHERE $timeFilter GROUP BY time($__interval), "domain") WHERE $timeFilter GROUP BY time($__interval)
# SELECT count("value") FROM "TestDB"."domains" WHERE $timeFilter GROUP BY time($__interval), "domain" fill(null)
query = 'select * from domains'
print(db.query(query, database='TestDB'))
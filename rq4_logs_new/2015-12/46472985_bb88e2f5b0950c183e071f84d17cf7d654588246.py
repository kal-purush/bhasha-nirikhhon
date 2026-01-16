"""Annual-Rainfal"""
import csv
def fetch_data():
  """bring"""
  with open('RainfallHourlyData2012_Part1.csv',newline='') as csvfile:
  data = csv.reader(csvfile)
  table=[row for row in data]
  info = {}
  for time in range(len(table)):
    rain = [float(i) for i in table[time][-23:]]
    if table[time][3] not in info:
      info[table[time][3]] = sum(rain)
    else:
      info[table[time][3]] += sum(rain)
  print(info)

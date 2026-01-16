import csv
import matplotlib.pyplot as plt
from datetime import datetime

# first_date = datetime.strptime('2018-07-01', '%Y-%m-%d')
# print(first_date)

filename = 'data/death_valley_2018_simple.csv'
with open(filename) as f:
    reader = csv.reader(f)
    header_row = next(reader)

    # get dates and high temperatures from the file

    dates, highs, lows = [], [], []

    
    for index, column_header in enumerate(header_row):
        print(index, column_header)

    # get high temp from the file
   
    for row in reader:
        current_date = datetime.strptime(row[2], '%Y-%m-%d')
        try:
            high = int(row[4])
            low = int(row[5])
        except ValueError:
            print(f'Missing data for {current_date}')
        else:
            dates.append(current_date)
            highs.append(high)
            lows.append(low)
    
plt.style.use('classic')
fig, ax = plt.subplots()
ax.plot(dates, highs, c='red', alpha=0.5)
ax.plot(dates, lows, c='blue', alpha=0.5)
plt.fill_between(dates, highs, lows, facecolor='blue', alpha=1)

title = 'Daily high and low temperatures during the year'
plt.title(title, fontsize=20)
plt.xlabel('', fontsize=16)
fig.autofmt_xdate()
plt.ylabel('Temperature', fontsize=16)
plt.tick_params(axis='both', which='major', labelsize=16)

plt.show()

import pandas as pd

data_frame = pd.read_csv('nyc_weather.csv')
#print(data_frame)

print('Max temperature:', data_frame['Temperature'].max())

print('Dates on which it rain: \n', data_frame['EST'][data_frame["Events"] == "Rain"])

data_frame.fillna(0,inplace=True)
print('Average wind speed:', data_frame['WindSpeedMPH'].mean())

print('Dates on which it rains and windspeed less than 8:', data_frame['EST'][data_frame["Events"]=="Rain"][data_frame['WindSpeedMPH']<8])
from weather import Weather

location = weather.lookup_by_location('halifax')
condition = location.condition()


for forecasts in location.forecast():
    print (forecasts['text'])
    print (forecasts['date'])
    print (forecasts['high'])
    print (forecasts['low'])

days=[]
high_temperature = []
low_temperature = []


current_condition = condition['text']

forecastslist = location.forecast()[:5]
for forecasts in forecastslist:
    days.append(forecasts['date'])
    high_temperature.append(forecasts['high'])
    low_temperature.append(forecasts['low'])
    forecast_string = str(forecasts['text'])

import asyncio
import aiohttp
import os
import datetime

urls = {
  'Toronto': "https://api.openweathermap.org/data/2.5/weather?lat=43.6532&lon=-79.3832&appid=" + os.environ['OPEN_WEATHER_KEY'],
  'Edmonton': "https://api.openweathermap.org/data/2.5/weather?lat=53.5461&lon=-113.4938&appid=" + os.environ['OPEN_WEATHER_KEY'],
  'Vancouver': "https://api.openweathermap.org/data/2.5/weather?lat=49.2827&lon=-123.1207&appid=" + os.environ['OPEN_WEATHER_KEY'],
  'Ottawa': "https://api.openweathermap.org/data/2.5/weather?lat=45.4215&lon=-75.6972&appid=" + os.environ['OPEN_WEATHER_KEY'],
  'Montreal': "https://api.openweathermap.org/data/2.5/weather?lat=45.5017&lon=-73.5673&appid=" + os.environ['OPEN_WEATHER_KEY'],
}

cachedWeather = {}

async def get(city, url, session):
    try:
        async with session.get(url=url) as response:
          resp = await response.json()
          print
          return {city: resp['weather'][0]['description'].capitalize()}
    except Exception as e:
        print("Unable to get url {} due to {}.".format(url, e.__class__))


async def getWeatherInfo():
  cached = 'time' in cachedWeather
  if cached:
    timeDifference = (datetime.datetime.now() - cachedWeather['time'])
    totalSeconds = timeDifference.total_seconds()
    minutes = totalSeconds/60

  # cache weather every 5 minutes
  if not cached or minutes > 1:
    # Run the requests parallely to save on request time
    ret = {}
    async with aiohttp.ClientSession() as session:
      data = await asyncio.gather(*[get(city, url, session) for city, url in urls.items()])
      for d in data:
          for k, v in d.items():
              ret[k] = v
      cachedWeather['weather'] = ret
      cachedWeather['time'] = datetime.datetime.now()
    return ret
  return cachedWeather['weather']
    
  
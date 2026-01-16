import sys
import datetime
from datetime import date, timedelta
import requests

#from mycroft import MycroftSkill, intent_file_handler
from pyowm import OWM, timeutils
import forecastio

# Cache provider to be used
#from pyowm.caches.lrucache import LRUCache
#cache = LRUCache()
from weather_utils import desc_code, wind_direction, cal_cloudbase
from weather_utils import mph_to_kmph, windscale, get_api_key
from weather_alerts import weather_alerts

# User input, keeping the scope to pune for PoC porposes(MVP).
city_name = 'pune' + ',in'
lat = 18.5195694
lng = 73.8553467


# Not utilising much of Darksky, just need to get dewpoint, for 
# cloud base calculations. Need to use it future for validating
# metrics and comparing wind and temperature.
def weather_current_darksky():
    darksky_api = get_api_key('darksky')
    forecast = forecastio.load_forecast(darksky_api, lat, lng)
    fc_cur = forecast.currently()

    # Return the cloud base height and the dewpoint
    dewpoint = fc_cur.dewPoint
    return(cal_cloudbase(fc_cur.temperature, dewpoint), dewpoint)


# Function to get the next 5 days forecast with darksky APIs
def weather_forecast_darksky(lat, lng):

    darksky_api = get_api_key('darksky')
    forecast = forecastio.load_forecast(darksky_api, lat, lng, units='si')
    # Print alerts if any
    alerts = forecast.alerts()
    if alerts: print("ALERTs: {}".format(alerts))

    print "===========Daily Data========="
    by_day = forecast.daily()
    print "Daily Summary: %s" % (by_day.summary)

    weekday = datetime.datetime.today()
    for aday in by_day.data:
        aday = dict(day = date.strftime(weekday, '%a'),
                    sum = aday.summary,
                    tempMin = aday.temperatureMin,
                    tempMax = aday.temperatureMax,
                    percipcnt = aday.precipProbability
                   )
        print('{day}: {sum} Temp range: {tempMin} - {tempMax}C'.format(**aday))
        weekday += timedelta(days=1)


# Function to take an observation list and do weather reporting.
def weather_current_owm():

    owm_api = get_api_key('owm')
    owm_obj = OWM(owm_api)
    obs_lst = owm_obj.weather_at_coords(lat, lng)
    # Check if API are online to make calls.
    if not owm_obj.is_API_online():
        print("OpenWeatherMap API is Not Online.\n")
        print("OpenWeatherMap Login Failed")
        sys.exit(1)

    # An Observation object will be returned, containing weather info about
    # the location matching the toponym/ID/coordinates you provided. 

    print("\n################################\n")
    print("Weather Observation received at: {}"\
        .format(obs_lst.get_reception_time(timeformat='iso')))

    forcast_loc = (obs_lst.get_location()).get_name()
    forcast = obs_lst.get_weather()
    wind_dir = wind_direction(forcast.get_wind())
    wind_kmph = int(round(mph_to_kmph(forcast.get_wind().get('speed'))))
    winds = str(wind_kmph) + " kmph " + windscale(wind_kmph)
    cloudbase, dewpoint = weather_current_darksky()

    print("Current Cloud Coverage {}%".format(forcast.get_clouds()))

    print("Estimated Cloudbase height is {} meters.".format(cloudbase))
    print("Current Rain volume in last 3 hours {} mm".format(forcast.get_rain().get('3h')))
    print("Current Wind Direction {} & Speed {}".format(wind_dir, winds))

    print("Current Humidity percentage {}%".format(forcast.get_humidity()))
    print("Current Atmospheric Pressure {} hPa and at sea-level {} hPa"\
        .format(forcast.get_pressure().get('press'), forcast.get_pressure().get('sea_level')))
    print("Current Temperature {} C, Min_temperature: {}C & Max_temperature: {}C"\
        .format(forcast.get_temperature(unit='celsius').get('temp'),\
            forcast.get_temperature(unit='celsius').get('temp_min'),\
                 forcast.get_temperature(unit='celsius').get('temp_max')))
    print("Current DewPoint measured is {} C".format(dewpoint))

    # Get OWM weather condition code
    code_means = forcast.get_detailed_status() + " " + desc_code(forcast.get_weather_code())
    print("Current Weather Status : {}".format(code_means))
    
    # Get METAR and TAF current weather for the location.
    weather_aviation('taf')
    # Get Terminal Aerodrome Forecase(TAF)
    weather_aviation('metar')

    print("\n###################################\n")


# Common function to get the forecast for the given city.
def weather_forecast_owm(city_name):

    print("--------- Weather forecast every 3 hours, for next 5 days -------")
    # Query for 3 hours weather forecast for the next 5 days
    weather_fcast = owm_obj.three_hours_forecast(city_name)
    
    print("Weather forecast starts: {} and ends: {}"\
        .format(weather_fcast.when_starts('iso'), weather_fcast.when_ends('iso')))

    forecasts = weather_fcast.get_forecast()
    # The weather objects returned inside a Forecast object 
    # may refer to timestamps in the recent past. In order to 
    # remove those outdated weather items from the forecast
    forecasts.actualize()

    # 24 hours weather forecast
    print("#"*25)
    print("Forecast reception time: {}".format(forecasts.get_reception_time('iso')))
    t = timeutils.tomorrow()
    print("24 hours weather from now: {}"\
        .format(weather_fcast.get_weather_at(t).get_detailed_status()))
    print("Today Rain forecast: {}".format(weather_fcast.will_have_rain()))
    print("Today Fog forecast: {}".format(weather_fcast.will_have_fog()))
    print("Today Cloud forecast: {}".format(weather_fcast.will_have_clouds()))
    print("Today Sun forecast: {}".format(weather_fcast.will_have_sun()))
    print("#"*25)

    prsnt = str(datetime.datetime.now().date())
    # Display the forecasts in interval of 3 hours for today.
    date_arr = [aweather.get_reference_time('iso') for aweather in forecasts]
    dtsplt = [adate.split(' ')[0] for adate in date_arr]
    # set() will store only unique values.
    date_set = set(dtsplt)
    uniq_dates = list(date_set)

    # Creating a list of lists to store details for each date.
    #series = [[] for i in range(len(uniq_dates))]
    #print("Depths:{}".format(series))
    #for aweather in forecasts:
    #    for auniq in uniq_dates:
    #        indx = uniq_dates.index(auniq)
    #        if auniq in aweather.get_reference_time('iso'):
    #            series[indx].append(aweather)

    #print("Weather Series:{}".format(series))

    hott = weather_fcast.most_hot()
    print("\nNext 5Days, the Hottest period is {} and {}"\
        .format(hott.get_reference_time('iso'), hott.get_detailed_status()))
    winds = weather_fcast.most_windy()
    print("Next 5Days, most windy period is {} and {}"\
        .format(winds.get_reference_time('iso'), winds.get_detailed_status()))
    rains = weather_fcast.most_rainy()
    # For days when there is no rains.
    if rains is not None:
        print("Next 5Days, most rains around {}"\
            .format(rains.get_reference_time(), rains.get_detailed_status()))
    humid = weather_fcast.most_humid()
    print("Next 5Days, Most humidity around {} and {}"\
        .format(humid.get_reference_time('iso'), humid.get_detailed_status()))
    snows = weather_fcast.most_snowy()
    if snows is not None:
        print("Next 5Days, most snow on {} and {}"\
            .format(snows.get_reference_time(), snows.get_detailed_status()))


# Utility function to get METAR & TAF information.
def weather_aviation(flag=''):
    # https://tgftp.nws.noaa.gov/data/observations/metar/stations/VAPO.TXT
    checkwx_api = get_api_key('checkwx')

    hdr = { 'X-API-Key': checkwx_api }
    response = requests.get('https://api.checkwx.com/station/vapo', headers=hdr).json()
    # Return the elevation for cloudbase calculations.
    if 'elevation' in flag:
        return(response['data'][0]['elevation']['meters'])
    elif 'metar' in flag:
        print("\n----- Decoded METAR from nearest station ------\n")
        # METARs typically come from airports or permanent weather observation stations. 
        # A typical METAR contains data for the temperature, dew point, wind direction and speed, 
        # precipitation, cloud cover and heights, visibility, and barometric pressure. 
        # A METAR may also contain information on precipitation amounts, lightning, and 
        # other information that would be of interest to pilots
        req = requests.get('https://api.checkwx.com/metar/vapo/decoded', headers=hdr).json()
        if req['results'] == 0:
            print("Pune(vapo) is NOT transmitting METAR now, getting from Mumbai")
            req = requests.get('https://api.checkwx.com/metar/lat/18.5195694/lon/73.8553467/decoded?pretty=1',\
                headers=hdr).json()
            print("METAR from {}({}), observed at {}".\
                format(req["data"][0]["station"]["name"], req["data"][0]["icao"],\
                    req["data"][0]["observed"]))
            print("RAW METAR: {}".format(req["data"][0]["raw_text"]))
            print("The temperature is {}C and DewPoint is {}C"\
                .format(req["data"][0]["temperature"]["celsius"], 
                req["data"][0]["dewpoint"]["celsius"]))
            print("The humidity is {}% & pressure is {}mb with visibility of {} meters.".\
                format(req["data"][0]["humidity"]["percent"], req["data"][0]["barometer"]["mb"],\
                    req["data"][0]["visibility"]["meters"]))
            print("The cloud ceiling is at {}m and {}({})"\
                .format(req["data"][0]["ceiling"]["meters_agl"], req["data"][0]["ceiling"]["text"],\
                    req["data"][0]["ceiling"]["code"]))
            low_clouds = "The sky conditions at level {}m is {}({})"\
                .format(req["data"][0]["clouds"][0]["base_meters_agl"], req["data"][0]["clouds"][0]["text"],\
                    req["data"][0]["clouds"][0]["code"])
            mid_clouds = "and at level {}m is {}({})".format(req["data"][0]["clouds"][1]["base_meters_agl"],\
                req["data"][0]["clouds"][1]["text"], req["data"][0]["clouds"][1]["code"])
            high_clouds = "and at level {}m is {}({})".format(req["data"][0]["clouds"][2]["base_meters_agl"],
                req["data"][0]["clouds"][2]["text"], req["data"][0]["clouds"][2]["code"])
            
            print("{} {} {}".format(low_clouds, mid_clouds, high_clouds))
            print("Flight Conditions are {}({}) and category is {}"\
                .format(req["data"][0]["conditions"][0]["text"], req["data"][0]["conditions"][0]["code"],\
                    req["data"][0]["flight_category"]))
        else:
            print(req)
    elif 'taf' in flag:
        print("\n--------- 12hours TAF Weather Forecast --------\n")
        req = requests.get('https://api.checkwx.com/taf/vapo/decoded?pretty=1', headers=hdr).json()
        print("TAF Raw Text: {}".format(req["data"][0]["raw_text"]))
        print("TAF weather forecast for {} from {} till {}"\
            .format(req["data"][0]["station"]["name"], req["data"][0]["timestamp"]["issued"],\
                req["data"][0]["timestamp"]["to"]))
        
        w_speed = mph_to_kmph(req["data"][0]["forecast"][0]["wind"]["speed_mph"])
        w_dir = wind_direction(req["data"][0]["forecast"][0]["wind"]["degrees"], 'taf')
        print("Wind forecast are, speed {}kmph, direction {} & Visibility {} meters"\
            .format(w_speed, w_dir, req["data"][0]["forecast"][0]["visibility"]["meters"]))
        
        low_clouds = "The sky conditions at level {}m is {}({})"\
            .format(req["data"][0]["forecast"][0]["clouds"][0]["base_meters_agl"],\
            req["data"][0]["forecast"][0]["clouds"][0]["text"],\
            req["data"][0]["forecast"][0]["clouds"][0]["code"])
        mid_clouds = "and at level {}m is {}({})"\
            .format(req["data"][0]["forecast"][0]["clouds"][1]["base_meters_agl"],\
            req["data"][0]["forecast"][0]["clouds"][1]["text"],\
            req["data"][0]["forecast"][0]["clouds"][1]["code"])
        high_clouds = "at level {}m is {}({})"\
            .format(req["data"][0]["forecast"][0]["clouds"][2]["base_meters_agl"],\
            req["data"][0]["forecast"][0]["clouds"][2]["text"],\
            req["data"][0]["forecast"][0]["clouds"][2]["code"])

        print("{} {} {}".format(low_clouds, mid_clouds, high_clouds))

        print("The forecast conditions are {}({})"\
            .format(req["data"][0]["forecast"][0]["conditions"][0]["text"], \
            req["data"][0]["forecast"][0]["conditions"][0]["code"]))    
    

    # Returns the latest Station information for multiple stations within
    # a specified radius of the parameters latitude and longitude.
    #req = requests.get('https://api.checkwx.com/station/lat/18.5195694/lon/73.8553467/radius/100', headers=hdr)
    #print(req.text)


# Analysis historical data of the given place.
def weather_history():
    hist = owm_obj.station_day_history(43003, 10)
    station_hist = hist.get_station_history()

    hist1 = owm_obj.weather_history_at_place('Pune,in')


# Boilerplate template to start it all.
if __name__ == "__main__":
    
    # City IDs can be retrieved using a registry.
    # TBD: Make it generic for around the world, keeping in local for now.
    #reg = owm_obj.city_id_registry()
    #city_id = reg.ids_for('Pune', country="IN", matching='nocase')
    #city_loc = reg.locations_for("Pune", country="IN", matching='nocase')
    #city_coords = reg.geopoints_for("Pune", country="IN", matching='nocase')
    #print("City ID: {}, City Loc: {} and City Geo Co-ords:\
    #        {}".format(city_id, city_loc, city_coords))

    # An Observation object will be returned, containing weather info about
    # the location matching the toponym/ID/coordinates you provided. 
    weather_current_owm()
    
    # Get weather forecast. Does it work with co-ordinates or name.
    #weather_forecast_owm(city_name)

    # Get Weather forecast with DARKSKY API. 
    # Get the next 5 days forecast from DarkSky API
    #weather_forecast_darksky(lat, lng)

    # TBD: PAID : Get weather history from meteoStations, by ID
    #weather_history()
    # TBD: Need to work on weather ALERTS
    #weather_alerts(owm_api, lat, lng)

    # TBD: Function to get adjoining areas weather.
    # obs_list = owm_obj.weather_at_places('Pune', 'accurate')
    # Find observed weather for all the places in the surroundings of
    # lon=-2.15,lat=57
    #obs_list = owm_obj.weather_around_coords(18.5195, 73.85534)
    # As above but limit result items to 8
    #obs_list = owm_obj.weather_around_coords(18.5195694,73.8553467, limit=8)
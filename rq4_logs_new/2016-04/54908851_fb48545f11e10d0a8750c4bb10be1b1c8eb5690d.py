# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 11:29:56 2016

@author: rabiagunonu
"""

from geolocation.google_maps import GoogleMaps

address = "New York City Wall Street 12"

google_maps = GoogleMaps(api_key='AIzaSyDXMRhwn0U_-YyNW6KYFteHB-TNmI-bCDg')

location = google_maps.search(location=address) # sends search to Google Maps.

print(location.all()) # returns all locations.

my_location = location.first() # returns only first location.

print(my_location.city)
print(my_location.route)
print(my_location.street_number)
print(my_location.postal_code)

for administrative_area in my_location.administrative_area:
    print("{}: {}".format(administrative_area.area_type, administrative_area.name))

print(my_location.country)
print(my_location.country_shortcut)

print(my_location.formatted_address)

print(my_location.lat)
print(my_location.lng)
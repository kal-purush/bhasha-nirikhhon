from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="Ashraf")

place = "27488 Stanford Avenue, North Dakota"

location = geolocator.geocode(place)

print(location.address)
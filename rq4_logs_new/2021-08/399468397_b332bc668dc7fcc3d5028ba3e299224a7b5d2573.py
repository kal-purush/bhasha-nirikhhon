station_name = 'Helsinki Kaivopuisto'
station_id = 132310
station_lat = 60.15
station_lon = 24.96
station_type = 'Mareographs'

type(station_name)
type(station_id)
type(station_lat)

station_id_str = str(station_id)
type(station_id_str)

station_name_and_id = station_name + ": " + str(station_id)
print(station_name_and_id)
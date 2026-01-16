import urllib.request
import json

class ElevationData:
    '''Base class for elevation data objects, which are responsible for 
    retreiving elevation data'''
    def __init__(self):
        self.base_url = None

    def is_healthy(self):
        '''Check if the data server is healthy and return a bool'''
        raise NotImplementedError('You need to define is_healthy()!')

    def get_elevations(self, lat_lng_list):
        '''Get the elevation at a given latitudes and longitudes'''
        raise NotImplementedError('You need to define get_elevation()!')

class OpenTopoData(ElevationData):
    '''Elevation data from opentopodata.org'''
    def __init__(self):
        self.base_url = 'https://api.opentopodata.org/'

    def is_healthy(self):
        contents = urllib.request.urlopen(self.base_url+'health').read()
        contents = json.loads(contents)
        return contents['status'] == 'OK'
    
    def get_elevations(self, lat_lng_list):
        # build the request by joining the locations
        dataset = 'aster30m'
        url = self.base_url + '/v1/' + dataset + '?locations='
        for i,lat_lng in enumerate(lat_lng_list):
            url += str(lat_lng[0]) + ',' + str(lat_lng[1])
            if i < len(lat_lng_list)-1:
                url += '|'

        # request the data
        contents = urllib.request.urlopen(url).read()
        contents = json.loads(contents)

        # extract a list of elevations
        elevations = []
        for result in contents['results']:
            elevations.append(result['elevation'])

        return elevations

class EPQSData(ElevationData):
    '''Elevation data from nationalmap.gov/epqs/'''
    def __init__(self):
        self.base_url = 'https://nationalmap.gov/epqs/'

    def get_elevations(self, lat_lng_list):
        elevations = []
        for lat_lng in lat_lng_list:
            url = self.base_url + 'pqs.php?'
            url += 'x=' + str(lat_lng[0]) + '&y=' + str(lat_lng[1])
            url += '&output=json&units=Meters'
        
            # request the data
            contents = urllib.request.urlopen(url).read()
            contents = json.loads(contents)

            # extract a list of elevations
            elevations.append(float(contents 
                ['USGS_Elevation_Point_Query_Service']
                ['Elevation_Query']['Elevation']))

        return elevations

if __name__ == "__main__":
    otd = OpenTopoData()
    print(otd.is_healthy())
    print(otd.get_elevations([(-43.5,172.5), (27.6,1.98)]))

    epqs = EPQSData()
    print(epqs.get_elevations([(36.1,-115.3)]))
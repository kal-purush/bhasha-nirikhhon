from math import radians, cos, sin, asin, sqrt
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import geopandas
from shapely.geometry import Point, Polygon
from datetime import datetime, timedelta
import bisect

import csv




def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers is 6371
    km = 6371* c
    conv_fac = 0.621371

    return km * conv_fac


class CheckMatch():
    def __init__(self, row1, row2):
        self.row1 = row1
        self.row2 = row2
        # check total number passengers <= taxi size (4)
        self.seat_available = (self.row1['passenger_count'] + self.row2['passenger_count']) <= 4

        # bearing difference smaller than a defined angle (3 degree)
        self.bearing_diff = abs(self.row1['bearing'] - self.row2['bearing']) < 3
        # distance between two pickups in mile
        self.pickup_distance = haversine(\
            self.row1['pickup_latitude'], self.row1['pickup_longitude'], \
            self.row2['pickup_latitude'], self.row2['pickup_longitude'])
        self.max_waittime2 = 5 # max waittime for ride2
        self.max_waittimeTaxi = 1 #max waittime for taxi to wait for ride2
        self.max_detour = 1.2
        self.origin_distance = self.row1['distance_line'] + self.row2['distance_line']


    def naive_overlap(self):
        '''
        Decide if two trips can be aggregated
        '''
        # check bearing difference and seat availbale
        if not (self.bearing_diff and self.seat_available):
            return False

        # check if pickup is reasonable
        self.pickup_overlap = self.check_pickup_overlap()
        if not self.pickup_overlap:
            return False


        # Assumption1: taxi always picks up rider1 first
        # Assumption2: taxi always drop off the rider(s) with the closer destination
        # make sure detour is not too much
        self.detour_ok = self.check_detour()
        if not self.detour_ok:
            return False

        return True




    def check_pickup_overlap(self):
        '''
        check if it makes sense to pick up two rides

        return: Boolean
        '''

        if self.pickup_distance >= self.row1['distance_line']:
            # distance of ride_1 is shorter than the distance between two pickups
            return False

        # estimate trip duration and car speed
        # convert duration to minutes
        trip_duration = self.row1['trip_duration'].total_seconds() / 60

        # estimate average car speed according to the trip record
        avg_car_speed = self.row1['trip_distance'] / trip_duration # miles per hour


        # time difference between two pickups in minute
        pickup_timediff = abs(self.row2['tpep_pickup_datetime'] - \
                              self.row1['tpep_pickup_datetime']).total_seconds() / 60

        # time difference between driving time from 1st to 2nd pickup and the pickup time difference
        pickup_wait = (self.pickup_distance / avg_car_speed - pickup_timediff)
        if pickup_wait > self.max_waittime2:
            # taxi will be late for at least 7 min when it arrives at 2nd pickup,
            # that is, 2nd rider will wait for more than 7 min
            return False
        elif pickup_wait < -self.max_waittimeTaxi:
            # taxi will be at least 1 min earlier when it arrivies at 2nd pickup,
            # that is, taxi will stop and wait for 2nd rider for more than 1 min
            return False
        else:
            return True


    def new_distance(self):
        '''
        Calculate new trip distances for each rider
        '''
        pick2_to_drop1 = haversine(\
            self.row2['pickup_latitude'], self.row2['pickup_longitude'],  \
            self.row1['dropoff_latitude'], self.row1['dropoff_longitude'])

        pick2_to_drop2 = self.row2['distance_line']
        pick1_to_pick2 = self.pickup_distance
        drop1_to_drop2 = haversine(\
            self.row1['dropoff_latitude'], self.row1['dropoff_longitude'], \
            self.row2['dropoff_latitude'], self.row2['dropoff_longitude'])

        if pick2_to_drop1 <= pick2_to_drop2: # taix drops ride_1 first
            # total distance for rider_1
            self.distance1 = pick1_to_pick2 + pick2_to_drop1
            # total distance for rider_2
            self.distance2 = pick2_to_drop1 + drop1_to_drop2

            #distance_total = pick1_to_pick2 + pick2_to_drop1 + drop1_to_drop2
            self.distance_total = self.distance1 + self.distance2 - pick2_to_drop1
        else: # taix drops ride_2 first
            self.distance1 = pick1_to_pick2 + pick2_to_drop2 + drop1_to_drop2
            self.distance2 = pick2_to_drop2
            self.distance_total = self.distance1



    def check_detour(self):
        '''
        Check if detour is tolerant

        return Boolean
        '''
        self.new_distance()

        if (self.distance1/self.row1['distance_line'] < self.max_detour\
            ) and (self.distance2/self.row2['distance_line'] < self.max_detour\
            ) and (self.distance_total < self.origin_distance):
            return True

        return False

def main():

    df_sorted = pd.read_csv('Manhattan201606FirstWeek_sorted.csv', parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

    df_sorted['trip_duration'] = df_sorted['tpep_dropoff_datetime'] - df_sorted['tpep_pickup_datetime']
    print('data loaded')

    matched = set() # store the aggregated rides
    max_time = timedelta(seconds=30)
    max_bucket_size = 200
    n = df_sorted.shape[0]
    print(n)

    with open('matched2.csv', 'w',encoding='utf-8') as file:
        csv_file = csv.writer(file)
        csv_file.writerow(('id', 'match_id', 'new_distance', 'tot_saved_mile'))
        for i, row1 in df_sorted.iloc[:n].iterrows():
            if i in matched:
                continue

            # search for the last trip that happened within max_time
            # and create the "bucket"
            x = np.datetime64(row1['tpep_pickup_datetime'] + max_time)
            right = bisect.bisect_right(df_sorted['tpep_pickup_datetime'].iloc[i+1: i+1+200].values, x)
            right = min(right, 201)

            for j, row2 in df_sorted.iloc[i+1: i+right].iterrows():
                if j in matched:
                    continue
                # check if two trips can be aggregated:
                matching = CheckMatch(row1, row2)
                if matching.naive_overlap():
                    matched.add(i)
                    matched.add(j)
                    print((i, j))

                    saved_mile = matching.origin_distance - matching.distance_total
                    csv_file.writerow((row1['index'], row2['index'], matching.distance1, saved_mile))
                    csv_file.writerow((row2['index'], row1['index'], matching.distance2, saved_mile))
                    break



if __name__ == '__main__':
    main()
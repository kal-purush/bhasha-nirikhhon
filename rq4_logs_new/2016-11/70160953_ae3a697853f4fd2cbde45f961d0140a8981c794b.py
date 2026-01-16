#!/usr/bin/env python
"""
pgoapi - Pokemon Go API
Copyright (c) 2016 tjado <https://github.com/tejado>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.

Author: tjado <https://github.com/tejado>
"""

import os
import json
import time
import logging
import argparse
import pprint
#import webbrowser
import urllib

from pgoapi import PGoApi
from pgoapi import utilities as util

from google.protobuf.internal import encoder
from geopy.geocoders import GoogleV3
from s2sphere import CellId, LatLng

log = logging.getLogger(__name__)

def get_pos_by_name(location_name):
    geolocator = GoogleV3()
    loc = geolocator.geocode(location_name)
    if not loc:
        return None

    log.info('Your given location: %s', loc.address.encode('utf-8'))
    log.info('lat/long/alt: %s %s %s', loc.latitude,
             loc.longitude, loc.altitude)

    return (loc.latitude, loc.longitude, loc.altitude)


def get_cell_ids(lat, longi, radius=10):
    origin = CellId.from_lat_lng(LatLng.from_degrees(lat, longi)).parent(15)
    walk = [origin.id()]
    right = origin.next()
    left = origin.prev()

    # Search around provided radius
    for i in range(radius):
        walk.append(right.id())
        walk.append(left.id())
        right = right.next()
        left = left.prev()

    # Return everything
    return sorted(walk)


def encode(cellid):
    output = []
    encoder._VarintEncoder()(output.append, cellid)
    return ''.join(output)


def init_config():
    parser = argparse.ArgumentParser()
    config_file = "config.json"

    # If config file exists, load variables from json
    load = {}
    if os.path.isfile(config_file):
        with open(config_file) as data:
            load.update(json.load(data))

    # Read passed in Arguments
    required = lambda x: x not in load
    parser.add_argument("-a", "--auth_service", help="Auth Service ('ptc' or 'google')",
                        required=required("auth_service"))
    parser.add_argument("-u", "--username", help="Username",
                        required=required("username"))
    parser.add_argument("-p", "--password", help="Password",
                        required=required("password"))
    parser.add_argument("-l", "--location", help="Location",
                        required=required("location"))
    parser.add_argument("-d", "--debug", help="Debug Mode",
                        action='store_true')
    parser.add_argument(
        "-t", "--test", help="Only parse the specified location", action='store_true')
    parser.set_defaults(DEBUG=False, TEST=False)
    config = parser.parse_args()

    # Passed in arguments shoud trump
    for key in config.__dict__:
        if key in load and config.__dict__[key] == None:
            config.__dict__[key] = load[key]

    if config.auth_service not in ['ptc', 'google']:
        log.error("Invalid Auth service specified! ('ptc' or 'google')")
        return None

    return config


def main():
    # log settings
    # log format
    logging.basicConfig(
        level=logging.DEBUG, format='%(asctime)s [%(module)10s] [%(levelname)5s] %(message)s')
    # log level for http request class
    logging.getLogger("requests").setLevel(logging.WARNING)
    # log level for main pgoapi class
    logging.getLogger("pgoapi").setLevel(logging.INFO)
    # log level for internal pgoapi class
    logging.getLogger("rpc_api").setLevel(logging.INFO)

    config = init_config()
    if not config:
        return

    if config.debug:
        logging.getLogger("requests").setLevel(logging.DEBUG)
        logging.getLogger("pgoapi").setLevel(logging.DEBUG)
        logging.getLogger("rpc_api").setLevel(logging.DEBUG)

    position = get_pos_by_name(config.location)
    if not position:
        return

    if config.test:
        return

    # instantiate pgoapi
    api = PGoApi()

    # provide player position on the earth
    api.set_position(*position)

    if not api.login(config.auth_service, config.username, config.password):
        return

    # chain subrequests (methods) into one RPC call

    # get player profile call
    # -----------------------
    time.sleep(1)
    response_dict = api.get_player()
    time.sleep(1)

    while True:
        find_poi(api, position[0], position[1])
        time.sleep(10)


def find_poi(api, lat, lng):
    forts = []
    # step_size = 0.0015
    # step_limit = 1
    api.set_position(lat, lng, 0)

    # get_cellid was buggy -> replaced through get_cell_ids from pokecli
    # timestamp gets computed a different way:
    cell_ids = get_cell_ids(lat, lng)
    timestamps = [0, ] * len(cell_ids)
    response_dict = api.get_map_objects(latitude=util.f2i(lat), longitude=util.f2i(lng), since_timestamp_ms=timestamps, cell_id=cell_ids)
    if (response_dict['responses']):
        if 'status' in response_dict['responses']['GET_MAP_OBJECTS']:
            if response_dict['responses']['GET_MAP_OBJECTS']['status'] == 1:
                for map_cell in response_dict['responses']['GET_MAP_OBJECTS']['map_cells']:
                    if 'forts' in map_cell:
                        for fort in map_cell['forts']:
                            if 'owned_by_team' in fort:
                                # It's a gym!
                                # print('Fort: \n\r{}'.format(pprint.PrettyPrinter(indent=4).pformat(fort)))
                                forts.append(fort)

    
    print_gmaps_dbug(forts)




def print_gmaps_dbug(fortlist):
    print('Fort: \n\r{}'.format(pprint.PrettyPrinter(indent=4).pformat(fortlist)))
    url_string = 'http://maps.googleapis.com/maps/api/staticmap?size=640x640&zoom=16&'
    url_string += 'style=feature:poi%7Celement:geometry%7Ccolor:0x4dff6d&'
    url_string += 'style=feature:all%7Celement:labels%7Cvisibility:off&'
    url_string += 'style=feature:road%7Cvisibility:simplified%7Celement:geometry%7Ccolor:0x51988f&'
    url_string += 'style=feature:landscape.man_made%7Cvisibility:on%7Celement:geometry.fill%7Ccolor:0x49e096&'
    url_string += 'style=feature:landscape.man_made%7Celement:geometry.stroke%7Ccolor:0x93ce92%7Cweight:1&'
    url_string += 'style=feature:landscape.natural%7Cvisibility:on%7Celement:geometry%7Ccolor:0x4dfe7c&'
    url_string += 'style=feature:poi.park%7Cvisibility:on%7Celement:geometry%7Ccolor:0x04aa90&'
    url_string += 'style=feature:water%7Cvisibility:on%7Celement:geometry%7Ccolor:0x1379d2&'
    url_string += 'style=feature:transit%7Cvisibility:off&'

    for fort in fortlist:
        url_string += 'markers=icon:http%3A%2F%2Fweb.mit.edu%2Fphilip%2Fwww%2Fgym%2F{}.png|'.format(fort['owned_by_team'])
        url_string += '{},{}&'.format(fort['latitude'], fort['longitude'])
        
        if 'is_in_battle' in fort:
            url_string += 'markers=icon:http%3A%2F%2Fweb.mit.edu%2Fphilip%2Fwww%2Fgym%2F0.png|'
            url_string += '{},{}&'.format(fort['latitude']+0.001, fort['longitude'])
        
           

    url_string += 'key=AIzaSyD4mQr7-Fxfw6Vg6RxnFpjDi65Wml_5MaM'
    print(url_string)
    print len(url_string)
    urllib.urlretrieve(url_string,"gym_map.png")
    #webbrowser.open(url_string, new = 0, autoraise = True)

if __name__ == '__main__':
    main()

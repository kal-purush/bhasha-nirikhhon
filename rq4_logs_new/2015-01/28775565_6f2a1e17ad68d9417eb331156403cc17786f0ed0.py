#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# multilogger - AREXX Multilogger communication library
# Copyright (c) 2014 Pirmin Kalberer
#
# This file is part of arexx-multilogger-collectd-plugin.
# arexx-multilogger-collectd-plugin is free software: you can
# redistribute it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, version 2.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, see <http://www.gnu.org/licenses/>.

#import os
#os.environ['PYUSB_DEBUG'] = 'debug'
try:
    import usb.core
    import usb.backend.libusb1
except:
    raise Exception("You need pyusb-1 to run this script!")
import array
import time
from datetime import datetime
import struct


class Multilogger:
    def __init__(self, action=None, rawAction=None):
        self.tx_endpoint = None
        self.action = action
        self.rawAction = rawAction
        self._req_clock = array.array('B', [0]*64)
        self._req_data = array.array('B', [0]*64)
        self._last_time_sync = datetime.fromtimestamp(0)

    def connect(self):
        try:
            dev = usb.core.find(idVendor=0x0451, idProduct=0x3211)
        except ValueError:
            # Backend isn't found when called from collectd
            # (at least libusb-1.0 on Arch)
            # So we set it manually
            backend = usb.backend.libusb1.get_backend(
                find_library=lambda x: "/usr/lib/libusb-1.0.so")
            dev = usb.core.find(idVendor=0x0451, idProduct=0x3211,
                                backend=backend)
        if not dev:
            raise Exception("Arexx Multilogger not found!")

        # set the active configuration. With no arguments, the first
        # configuration will be the active one
        dev.set_configuration()

        # get an endpoint instance
        cfg = dev.get_active_configuration()
        intf = cfg[(0, 0)]

        self.tx_endpoint = usb.util.find_descriptor(
            intf,
            # match the first OUT endpoint
            custom_match = \
            lambda e: \
                usb.util.endpoint_direction(e.bEndpointAddress) == \
                usb.util.ENDPOINT_OUT)
        if not self.tx_endpoint:
            raise Exception("Could'n get tx endpoint")

        self.rx_endpoint = usb.util.find_descriptor(
            intf,
            # match the first IN endpoint
            custom_match = \
            lambda e: \
                usb.util.endpoint_direction(e.bEndpointAddress) == \
                usb.util.ENDPOINT_IN)
        if not self.rx_endpoint:
            raise Exception("Could'n get rx endpoint")

    def send_and_receive(self, request, timeout = 200):
        self.tx_endpoint.write(request, timeout)
        #TODO: handle timeout/error
        time.sleep(0.001)
        raw_data = self.rx_endpoint.read(64, timeout)
        #TODO: handle timeout/error
        return raw_data

    TIME_OFFSET = 946681200  # Timestamp of 2000-01-01 00:00:00

    def set_clock(self):
        self._req_clock[0] = 4
        ts = int(time.time()) - self.TIME_OFFSET
        self._req_clock[1:5] = array.array('B', struct.pack("I", ts))
        self.send_and_receive(self._req_clock)

    def read_data_buffer(self):
        if not self.tx_endpoint:
            self.connect()
        if (datetime.now() - self._last_time_sync).total_seconds() > 900:
            self.set_clock()
            self._last_time_sync = datetime.now()
        self._req_data[0] = 3  # request data
        receiving_data = True
        while receiving_data:
            raw_data = self.send_and_receive(self._req_data)
            receiving_data = (raw_data[1] != 0)
            if receiving_data:
                if self.rawAction:
                    self.rawAction(raw_data)
                data = self._processed_data(raw_data)
                if self.action:
                    self.action(data)
                time.sleep(0.005)

    def collect_data(self, max_errors=50):
        errors = 0
        while errors <= max_errors:
            try:
                self.read_data_buffer()
                time.sleep(4)
            except Exception, e:
                print e
                errors += 1

    def _processed_data(self, raw_data):
        """ Extract data from byte array """
        sensor = raw_data[3]*256 + raw_data[2]
        valueRaw = raw_data[4]*256 + raw_data[5]
        clock_raw = raw_data[9]*16777216 + raw_data[8]*65536 + \
            raw_data[7]*256 + raw_data[6]
        clock = datetime.fromtimestamp(clock_raw + self.TIME_OFFSET)
        # From arexxd.c:
        # The raw values are transformed this way:
        # - sign-extend if signed
        # - apply the transformation polynomial
        # - apply the scaling function
        # - drop if outside the interval [vLo,vUp]
        # double z = raw;
        # double hi, lo;
        # char *unit;
        # int idhi = id & 0xf000;
        #
        # if (idhi == 0x1000) {
        #     z = 0.02*z - 273.15;
        #     lo = -200;
        #     hi = 600;
        #     unit = "C";
        # } else if (idhi == 0x2000) {
        #     if (raw >= 0x8000)
        #         z -= 0x10000;
        #     z /= 128;
        #     lo = -60;
        #     hi = 125;
        #     unit = "C";
        # } else if (idhi == 0x4000) {
        #     if (!(id & 1)) {
        #         z = z/100 - 39.6;
        #         lo = -60;
        #         hi = 125;
        #         unit = "C";
        #     } else {
        #         z = -2.8e-6*z*z + 0.0405*z - 4;
        #         lo = 0;
        #         hi = 100.1;
        #         unit = "%RH";
        #     }
        # } else if (idhi == 0x6000) {
        #     if (!(id & 1)) {
        #         if (raw >= 0x8000)
        #             z -= 0x10000;
        #         z /= 128;
        #         lo = -60;
        #         hi = 125;
        #         unit = "C";
        #     } else {
        #         z = -3.8123e-11*z;
        #         z = (z + 1.9184e-7) * z;
        #         z = (z - 1.0998e-3) * z;
        #         z += 6.56;
        #         z = pow(10, z);
        #         lo = 0;
        #         hi = 1e6;
        #         unit = "ppm";
        #     }
        # } else {
        #     log_error("Unknown sensor type 0x%04x", id);
        #     return;
        # }

        # if (z < lo || z > hi) {
        #     log_error("Sensor %d: value %f out of range", id, z);
        #     return;
        # }
        value = 0.0078 * valueRaw
        return clock, sensor, valueRaw, value


if __name__ == "__main__":
    def log(data):
        print "\t".join([str(i) for i in data])

    tl500 = Multilogger(action=log)
    tl500.collect_data()
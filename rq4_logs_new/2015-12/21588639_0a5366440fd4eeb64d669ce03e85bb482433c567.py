#!/usr/bin/env python
# coding=utf8
import time
import logging
import rrdtool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

rrd_dir = '/home/dinomite/data/'


def create_rrd(filename, data_source):
    ret = rrdtool.create(rrd_dir + filename + '.rrd',
                         "--step", "60",
                         "--start", time.strftime('%s'),
                         data_source,
                         # Every minute for a year
                         "RRA:AVERAGE:0.5:1:525600",
                         # Hourly average, 5 years
                         "RRA:AVERAGE:0.5:60:43800",
                         # Min annual temperature
                         "RRA:MIN:0.5:1440:365",
                         # Max annual temperature
                         "RRA:MAX:0.5:1440:365")
    if ret:
        logger.error("Error creating RRD: " + rrdtool.error())
        exit(1)


# rrdtool.create() is supposed to be able to take an array of data sources.
# Instead, it segfaults
create_rrd('desk', ["DS:temperature:GAUGE:60:50:90", "DS:pressure:GAUGE:60:87000:108570"])
ret = rrdtool.create(rrd_dir + 'desk.rrd',
                     "--step", "60",
                     "--start", time.strftime('%s'),
                     "DS:temperature:GAUGE:60:50:90",
                     "DS:pressure:GAUGE:60:87000:108570",
                     # Every minute for a year
                     "RRA:AVERAGE:0.5:1:525600",
                     # Hourly average, 5 years
                     "RRA:AVERAGE:0.5:60:43800",
                     # Min annual temperature
                     "RRA:MIN:0.5:1440:365",
                     # Max annual temperature
                     "RRA:MAX:0.5:1440:365")
create_rrd('outside', "DS:temperature:GAUGE:60:-25:125")
create_rrd('vent', "DS:temperature:GAUGE:60:35:120")
exit(0)
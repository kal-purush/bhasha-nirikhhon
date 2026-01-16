#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import fnmatch
import time
import MySQLdb as mdb
import logging
import subprocess
import urllib
from BeautifulSoup import *
import re
import Adafruit_BMP.BMP085 as BMP085
import Adafruit_DHT as dht

#### DS18B20 sensor
logging.basicConfig(filename='/home/pi/DS18B20_error.log',
  level=logging.DEBUG,
  format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

# Load the modules (not required if they are loaded at boot) 
os.system('modprobe w1-gpio')
os.system('modprobe w1-therm')

# Function for storing readings into MySQL
def insertDB(IDs, temperature):

  try:

    con = mdb.connect('192.168.0.25',
                      'pi_insert',
                      '4455stat',
                      'measurements');
    cursor = con.cursor()

    for i in range(0,len(temperature)):
      sql = "INSERT INTO all_temp(temperature, sensor_id) \
      VALUES ('%s', '%s')" % \
      ( temperature[i],IDs[i])
      cursor.execute(sql)
      sql = []
      con.commit()

    con.close()

  except mdb.Error, e:
    logger.error(e)

# Get readings from sensors and store them in MySQL
temperature = []
IDs = []

for filename in os.listdir("/sys/bus/w1/devices"):
  if fnmatch.fnmatch(filename, '28-*'):
    with open("/sys/bus/w1/devices/" + filename + "/w1_slave") as f_obj:
      lines = f_obj.readlines()
      if lines[0].find("YES"):
        pok = lines[1].find('=')
        temperature.append(float(lines[1][pok+1:pok+7])/1000)
        IDs.append(filename)
      else:
        logger.error("Error reading sensor with ID: %s" % (filename))

if (len(temperature)>0):
  insertDB(IDs, temperature)

##### Returns the temperature in degrees C of the CPU
temperature = []
IDs = ['Pi_CPU-00000000']
dir_path = "/opt/vc/bin/vcgencmd"
s = subprocess.check_output([dir_path,"measure_temp"])
temperature.append(float(s.split("=")[1][:-3]))

if (len(temperature) > 0):
  insertDB(IDs, temperature)

###### Returns the temperature in degrees from Environment Canada
temperature = []
IDs = ['weather.gc.ca']

url = 'https://weather.gc.ca/rss/city/on-118_e.xml'
xml = urllib.urlopen(url).read()
soup = BeautifulSoup(xml)

# Retrieve all of the anchor tags
tags = soup('title')
for tag in tags:
	# Look at the parts of a tag
	if re.search('^Current Conditions',tag.contents[0]):
		temperature.append(float(re.findall('([-0-9.]+)',tag.contents[0])[0])/1.0)

if (len(temperature)>0):
  insertDB(IDs, temperature)

###### BMP180 sensor with temperature and pressure
logging.basicConfig(filename='/home/pi/bmp180_error.log',
  format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

# Get readings from sensor and store them in MySQL
sensor = BMP085.BMP085()

temperature =[]
IDs = ['BMP180']
temperature.append(sensor.read_temperature())

if (len(temperature) > 0):
	insertDB(IDs,temperature)

###### DHT22 sensor and store them in MySQL

temperature =[]
temperature.append(dht.read_retry(dht.DHT22,26)[1])
IDs = ['DHT22']

insertDB(IDs,temperature)
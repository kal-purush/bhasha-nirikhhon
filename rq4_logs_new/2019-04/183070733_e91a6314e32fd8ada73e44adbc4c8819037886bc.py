import time
import os
import configparser
from tcp.client import HubClient
from db.db import DB
import deep_od_lib
#from deep_od_lib import *

db = DB()
client = HubClient()
config = configparser.ConfigParser()
path = os.path.abspath(os.path.dirname(__file__)) #get current path from python script name
os.chdir(path) # set current working directory (path to look in) to same directory as script

ran = False


detections = {"person": 0, "horse": 0, "dog": 0, "car": 0, "bicycle": 0}
def image_detect_collect_counts(object_name,confidence):
	#detections.append(object)
	if object_name in detections.keys():
		detections[object_name] += 1

def get_images(sensor_id,ip):
	global detections
	client.connect(ip, 1234)
	client.get_sensor_images() # get all sensor images and add them to ./images
	client.disconnect()
	image_names = [name for name in os.listdir('images') if os.path.isfile("images/" + name)]
	detections = {"person": 0, "horse": 0, "dog": 0, "car": 0, "bicycle": 0}
	for image_name in image_names:
		detect_image(os.path.join("images",image_name), 0.75, image_detect_collect_counts)
	timestamp = "2019-01-01 01:01:01" #TODO: retrieve timestamp somehow!
	db.createCounts(sensor_id, timestamp, detections['person'], detections['horse'], detections['dog'], detections['car'], detections['bicycle'], 0)

def get_images_all_sensors():
	results = db.query("SELECT id,ip FROM sensors")
	for row in results:
		sensor_id = row[0];
		ip = row[1]
		get_images(sensor_id,ip)


while True:
	print("start")
	t = time.localtime()
	h = t.tm_hour
	m = t.tm_min

	config.read(os.path.join(path,"scheduler.ini")) # refresh config file entries
	test_h = int(config['scheduler']['hour'])
	test_m = int(config['scheduler']['minute'])

	print("%d %d =? %d %d" % (h,m,test_h,test_m))

	if h==test_h and m == test_m and not ran:
		ran=True
		get_images_all_sensors()
	if (h!=test_h or m != test_m) and ran:
		ran=False




	print("sleep")

	time.sleep(1)
	print("end")
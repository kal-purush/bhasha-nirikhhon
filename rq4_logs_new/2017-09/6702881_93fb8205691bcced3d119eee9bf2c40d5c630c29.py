#!/usr/bin/python
import serial
import sys
import logging
import paho.mqtt.client as mqtt
import yaml

with open('config.yaml') as config_f:
    config = yaml.safe_load(config_f)

if not config:
    print("Lol you need a config.yaml mate")
    sys.exit(1)

ser = serial.Serial(config['serial']['port'], config['serial']['baud'], timeout=0.5)

# We want to use a logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(message)s')

# Anything DEBUG or higher should be logged to disk
fh = logging.FileHandler('fred.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

# Anything DEBUG or higher should be spat out to the console
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

logging.info("FRED 1.0")

mqttc = mqtt.Client(config['mqtt']['name'])
mqttc.connect(config['mqtt']['server'], 1883, 60)
mqttc.will_set("system/%s/state" % config['mqtt']['name'], payload='offline', qos=0, retain=True)
mqttc.publish("system/%s/state" % config['mqtt']['name'], payload='online', qos=0, retain=True)
mqttc.publish("door/%s/rebooted" % config['door']['name'])
mqttc.loop_start()

ser.write('E')

while True:
    card_id = ser.readline().strip()

    if card_id:
        logging.debug(card_id)

        if card_id == 'D0-0':
            logging.info('Door Button Pressed')
            mqttc.publish("door/%s/opened/button" % config['door']['name'], qos=2)
            ser.write('1')

        if (card_id[0] == 'C'):
            card_id = card_id[1:]
            if card_id[0:2] == '88':
                card_id = card_id[2:]

            logging.info("Card ID: %s", card_id)
            found = False

            with members_f as open('members', 'r'):
                for member in members_f.readlines():
                    member = member.strip().split(',')
                    if member[0].startswith(card_id):
                        ser.write('1')
                        ser.write('G')
                        found = True
                        logging.info("%s found, %s opened the door!", card_id, member[1])
                        mqttc.publish("door/%s/opened/username" % config['door']['name'], member[1], qos=2)

            if not found:
                ser.write('R')
                logging.info("%s not found!", card_id)
                mqttc.publish("door/%s/invalidcard" % config['door']['name'], qos=2)
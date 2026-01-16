#!/usr/bin/env python
# Description: The script allows to enable the debug mode in the 
# arduino client.
# Author: Edward U. Benitez Rendon
# Date: 20-05-17

import paho.mqtt.client as mqtt
import time
import argparse

parser = argparse.ArgumentParser(description='Enable Debug Mode in Modules.')
parser.add_argument('board', type=int,
                    help='Specify the board to enable debug mode')
parser.add_argument('debug', type=int,
                    help='Enable debug mode')

args = parser.parse_args()
print(args.debug)
print(args.board)

mqttc = mqtt.Client("debugMode")
mqttc.connect("192.168.1.136", 1883)

if (args.board == 1):
	mqttc.publish("myHome/board/room01/debug", str(args.debug))
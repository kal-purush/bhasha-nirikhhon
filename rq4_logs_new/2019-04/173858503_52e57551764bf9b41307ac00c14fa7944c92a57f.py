#!/usr/bin/env python3

import sys
import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

import paho.mqtt.publish as publish
import paho.mqtt.subscribe as subscribe
from yaml import load

from time import sleep
import os
import threading

script_file = os.path.realpath(__file__)
yaml_file = os.path.join(os.path.dirname(script_file), '../secrets.yaml')

data = None
logging.info('parse secrets.yaml')
with open(yaml_file, 'r') as f:
    data = load(f)

MQTT_AUTH = {
    'username': data['mqtt_username'],
    'password': data['mqtt_password']
}

flag = True

def wait_for_touchpad():
    global flag
    logger = logging.getLogger('touchpad')
    logger.info('subscribe topic')
    subscribe.simple('hass/rpi/touchpad',
        hostname=data['mqtt_broker'],
        port=data['mqtt_port'],
        auth=MQTT_AUTH) 
    logger.info('touchpad pressed')
    flag = False


def notify():
    global flag
    logger = logging.getLogger('notify')

    interval = float(sys.argv[1])
    msg = ' '.join(sys.argv[2:])
    logger.info('the message is "%s"', msg)

    cnt = 0
    while flag: 
        cnt = cnt + 1
        logger.info('[%d] trigger', cnt)
        publish.single('/hass/util/admin/notify', '[{}] {}'.format(cnt, msg),
            hostname=data['mqtt_broker'],
            port=data['mqtt_port'],
            auth=MQTT_AUTH)
        sleep(interval)
    logger.info('stop notification')
    publish.single('/hass/util/admin/notify', 'alright',
        hostname=data['mqtt_broker'],
        port=data['mqtt_port'],
        auth=MQTT_AUTH)


if __name__ == "__main__":
    threads = []
    threads.append(threading.Thread(target=notify))
    threads.append(threading.Thread(target=wait_for_touchpad))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    logging.info('exit')

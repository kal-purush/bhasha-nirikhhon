import threading
import logging
import os
import time
import json
import datetime
import paho.mqtt.client as mqtt
from pymongo import MongoClient

logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger('sensor_listener')

class Listener(threading.Thread):
    """ A thread that iterates the mqtt loop and passes messages to
    whatever handler objects happen to be registered.
    Handler objects are responsible only for
    1. having topic and qos properties
    2. having a put() method
    sw.mqtt.handlers.BaseHandler is an abstract base class which enforces those
    When such an object is registered as a handler, it's msg_queue will receive all the raw
    messages from it's topic and in its run() method it can do whatever processing it wants.
    Can be constructed with a list of handlers or not

    The @staticmethods below are written that way so we can hide their names inside the class
    and still have access to self.
    """

    def __init__(self):
        super(self.__class__, self).__init__(name=self.__class__.__name__)
        self.daemon = True
        self._stop = threading.Event()
        self.mongoc = MongoClient()
        self.mongo_sensors = self.mongoc.sensors
        self.mqttc = mqtt.Client(client_id='listener', clean_session=False, userdata=self) # we pass an instance reference around to callbacks
        self.mqttc.on_connect = self._on_connect
        self.mqttc.on_message = self._on_message
        self.mqttc.on_subscribe = self._on_subscribe
        self.mqttc.on_disconnect = self._on_disconnect

    @staticmethod
    def _on_connect(client, self, flags, rc):
        """Can't subscribe to a topic until after we're connected"""
        if rc == mqtt.MQTT_ERR_SUCCESS:
            LOGGER.info("Successfully connected, subscribing to sensors channel")
            self.refresh_subscriptions()
        else:
            LOGGER.error("on_connect: received an error: %s", mqtt.connack_string(rc))

    @staticmethod
    def _on_subscribe(client, self, mid, granted_qos):
        """Just to print a log message for now"""
        LOGGER.info("subscription mid: %s, granted_qos: %s", mid, granted_qos)

    @staticmethod
    def _on_disconnect(client, self, rc):
        """If we get disconnected for whatever reason, let's try to reconnect"""
        LOGGER.info("Disconnect rc: %s, trying reconnect.", rc)
        self.mqttc.reconnect()

    @staticmethod
    def _on_message(client, self, msg):
        """Look through our handlers and pass on appropriate messages"""
        LOGGER.info("on_message: topic: %s, msg: %s", msg.topic, msg.payload)
        try:
            data = json.loads(msg.payload)
            if msg.topic == '/sensors/wormbin':
                if data['status'] == 0:
                    data.pop('status')
                    data['location'] = 'wormbin'
                    data['date'] = datetime.datetime.now()
                    data_id = self.mongo_sensors.environment.insert_one(data).inserted_id
                LOGGER.info('mongo id: %s', data_id)
        except Exception as e:
            LOGGER.error("Some problem with enqueuing message: %s", e)

    def refresh_subscriptions(self):
        """Utility method for re-subscribing"""
        self.mqttc.subscribe('/sensors/#', 0)

    def stop(self):
        self._stop.set()

    def stopped(self):
        return self._stop.isSet()

    def run(self):
        """All we do here is pump the mqtt network loop until we're asked to stop"""
        try:
            LOGGER.debug("trying to connect to mqtt server")
            self.mqttc.connect("localhost")
        except Exception as e:
            LOGGER.exception("Couldn't start mqtt message dispatcher: %s", e)

        while not self.stopped():
            self.mqttc.loop()

if __name__ == '__main__':
    LOGGER.info('Start MQTT sensor listener...')
    listener = Listener()
    listener.start()
    while True:
       time.sleep(60)
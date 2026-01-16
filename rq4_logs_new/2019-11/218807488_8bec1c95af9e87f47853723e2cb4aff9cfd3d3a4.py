#!/usr/bin/env python3.7

from snipsTools import SnipsConfigParser
from hermes_python.hermes import Hermes
from hermes_python.ontology import *
import io
import requests

CONFIG_INI = "config.ini"

MQTT_IP_ADDR: str = "localhost"
MQTT_PORT: int = 1883
MQTT_ADDR: str = "{}:{}".format(MQTT_IP_ADDR, str(MQTT_PORT))

class Lights(object):

    def __init__(self):
        try:
            self.config = SnipsConfigParser.read_configuration_file(CONFIG_INI)
        except Exception:
            self.config = None

        self.start_blocking()

    def controlDevice_callback(self, hermes, intent_message):

        if intent_message.slots.lights:
            light = intent_message.slots.lights.first().value
        if intent_message.slots.onOff:
            action = intent_message.slots.onOff.first().value
        token = self.config.get("secret").get("bearer-auth-token")
        api = self.config.get("secret").get("rest-api-url")
        auth = 'Bearer ' + token.encode("utf-8")
        header = {'Authorization': auth, 'Content-Type': 'application/json'}
        DeviceIDs = ['09b0803c-cfe3-4b8a-8fc0-e8161701ade4', '2537cac5-2c28-454f-bdc2-5741ae4c44c4',  '7c792f42-a018-4664-af72-d41cf93b49df', '1d9b3329-4d6f-493d-baac-1ee84cec75e8', '5d5a7635-f1a5-40ac-ad1f-d75967545824']

        if light == "lights":
            target = "all_lights"
        elif light == "lamps":
            target = "lamps"
        else:
            target == "one_light"
        if target == "all_lights":
            if action == "on" or action == "off":
                for index in range(len(DeviceIDs)):
                    uri=api.encode("utf-8") + '/device/' + DeviceIDs[index] + '/command/' + action.encode("utf-8")
                    print(url)
                    print(header)
                    response = get(uri, headers=header)
                hermes.publish_end_session(intent_message.session_id, "Turning " + action.encode("utf-8") + " " + light.encode("utf-8"))
        else:
            hermes.publish_end_session(intent_message.session_id, "Be boop be be boop, somethings not right")

    def master_intent_callback(self,hermes, intent_message):
        coming_intent = intent_message.intent.intent_name
        if coming_intent == 'hooray4me:powerToggleDevice':
            self.controlDevice_callback(hermes, intent_message)

    def start_blocking(self):
        with Hermes(MQTT_ADDR) as h:
            h.subscribe_intents(self.master_intent_callback).start()

if __name__ == "__main__":
    Lights()
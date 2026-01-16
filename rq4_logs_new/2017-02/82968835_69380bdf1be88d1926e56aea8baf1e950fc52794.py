import paho.mqtt.client as mqtt
import time

broker_address="localhost"
mqtt_client = mqtt.Client("arduino_message_handler")

HEATER_CTRL_QUEUE = "heater_ctrl"
ARDUINO_INCOMING_QUEUE = "arduino/incoming"
ARDUINO_OUTGOING_QUEUE = "arduino/outgoing"
DOOR_SENSOR_QUEUE = "sensors/door"
WINDOW_SENSOR_QUEUE = "sensors/window"


def handle_temperature(data):
    print("Temperature: {}".format(data))
    try:
        mqtt_client.publish('temperature', float(data))
    except ValueError:
        print("foobar data")


def handle_heater(data):
    print("Heater: {}".format(data))
    mqtt_client.publish('heater_state', data)


def handle_door(data):
    print("Door: {}".format(data))
    mqtt_client.publish(DOOR_SENSOR_QUEUE, data)


def handle_window(data):
    print("Window: {}".format(data))
    mqtt_client.publish(WINDOW_SENSOR_QUEUE, data)

TYPE_CODES = {
    'TMP': handle_temperature,
    'HTR': handle_heater,
    'DOR': handle_door,
    'WND': handle_window
}

HEATER_CMD_MAP = {
    "ON": "HTRON",
    "OFF": "HTROFF"
}


def on_connect(client, userdata, flags, rc):
    print("Connected")


def on_message(client1, userdata, message):
    payload = message.payload.decode("utf-8")
    print("message {} receieve from topic {}".format(payload, message.topic))
    if message.topic == HEATER_CTRL_QUEUE:
        mqtt_client.publish(ARDUINO_OUTGOING_QUEUE, HEATER_CMD_MAP[payload])
    elif message.topic == ARDUINO_INCOMING_QUEUE:
        type_code = payload[:3]
        handler = TYPE_CODES.get(type_code)
        if handler:
            handler(payload[3:].strip())

mqtt_client.on_connect= on_connect
mqtt_client.on_message=on_message
mqtt_client.connect(broker_address)
mqtt_client.subscribe([(ARDUINO_INCOMING_QUEUE, 0), (HEATER_CTRL_QUEUE, 0)])
mqtt_client.loop_forever()
import network
import time
import utime
import ntptime
from util import create_mqtt_client, get_telemetry_topic, get_c2d_topic, parse_connection
from sas_token_generator import GenerateAzureSasToken
import json
from water_sensor import WaterFlowSensor

print("Connecting to WiFi", end="")

sta_if = network.WLAN(network.STA_IF)
sta_if.active(True)
sta_if.connect('Wokwi-GUEST', '')

while not sta_if.isconnected():
    print(".", end="")
    time.sleep(0.1)
print(" Successfully connected!")

ntptime.host = '3.ph.pool.ntp.org'
ntptime.settime()

print("Local time after synchronization: %s" % str(time.localtime()))

HOST_NAME = "HostName"
SHARED_ACCESS_KEY_NAME = "SharedAccessKeyName"
SHARED_ACCESS_KEY = "SharedAccessKey"
SHARED_ACCESS_SIGNATURE = "SharedAccessSignature"
DEVICE_ID = "DeviceId"
MODULE_ID = "ModuleId"
GATEWAY_HOST_NAME = "GatewayHostName"

connection_str = "<connection-str>"

# Parse the connection string into constituent parts
dict_keys = parse_connection(connection_str)
shared_access_key = dict_keys.get(SHARED_ACCESS_KEY)
shared_access_key_name = dict_keys.get(SHARED_ACCESS_KEY_NAME)
gateway_hostname = dict_keys.get(GATEWAY_HOST_NAME)
hostname = dict_keys.get(HOST_NAME)
device_id = dict_keys.get(DEVICE_ID)
module_id = dict_keys.get(MODULE_ID)

# Create username following the below format '<HOSTNAME>/<DEVICE_ID>'
username = hostname + '/' + device_id

expiry_timestamp = time.time() + 946684800 + 3600 * \
    20  # Since Jan 1, 2000 + UNIX Epoch

password = GenerateAzureSasToken(
    hostname, shared_access_key, expiry_timestamp, policy_name="iothubowner")
print("SAS Token: %s" % password)

# Create UMQTT ROBUST or UMQTT SIMPLE CLIENT
mqtt_client = create_mqtt_client(client_id=device_id, hostname=hostname,
                                 username=username, password=password, port=8883, keepalive=120, ssl=True)

print("Connecting to MQTT Server...")
mqtt_client.reconnect()


def callback_handler(topic, message_receive):
    print("Received message")
    print(message_receive)


subscribe_topic = get_c2d_topic(device_id)
mqtt_client.set_callback(callback_handler)
mqtt_client.subscribe(topic=subscribe_topic)

print("Publishing topics...")
topic = get_telemetry_topic(device_id)

# Send telemetry

water_sensors = [
    WaterFlowSensor(34, "sensor-a"),
    WaterFlowSensor(35, "sensor-b"),
    WaterFlowSensor(32, "sensor-c")
]

while True:
    for water_sensor in water_sensors:
        rate, volume = water_sensor.read()
        tag = water_sensor.tag

        print("%s = %d L/hr, %f L" % (tag, rate, volume))

        mqtt_client.publish(topic=topic, msg=json.dumps({
            'flowRate': rate,
            'volume': volume,
            'tag': tag
        }))

        time.sleep(1)
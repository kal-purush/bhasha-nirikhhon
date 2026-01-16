import paho.mqtt.client as mqtt  # import the client1

from pyzabbix import ZabbixAPI, ZabbixMetric, ZabbixSender

broker_address = "192.168.0.190"
zapi = ZabbixAPI('http://zabbix-web-apache-mysql/', user='api', password='api')
item_id = '28666'


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("nodemcu/litres")


def zbx_set(metric):
    packet = [ZabbixMetric('Microserver', 'room.water.meter.litres', metric)]
    result = ZabbixSender(use_config=False, zabbix_server='zabbix-server').send(packet)
    print(result)


def on_message(client, userdata, message):
    print("message received ", str(message.payload.decode("utf-8")))
    print("message topic=", message.topic)
    print("message qos=", message.qos)
    print("message retain flag=", message.retain)

    payload_list = str(message.payload.decode("utf-8")).split(" ")
    mqtt_value = int(payload_list[1])
    # print(int(payload_list[1]))
    # print(int(payload_list[3]))

    history = zapi.history.get(itemids=item_id,
                               output='extend',
                               limit=1,
                               history=0,
                               sortfield='clock',
                               sortorder='DESC',
                               )
    # print(history)
    for point in history:
        # print("{0}".format(point['value']))
        # value = "{0}".format(point['value'])
        value = point['value'].split('.')
        print(value)

    zbx_value = int(value[0])

    print(zbx_value)
    print(mqtt_value)

    # zbx_set(mqtt_value)

    if zbx_value < mqtt_value:
        print "New value: %s" % mqtt_value
        zbx_set(mqtt_value)
    else:
        corrected_value = zbx_value + 1
        print "Corrected value: %s" % corrected_value
        zbx_set(corrected_value)
        client.publish("nodemcu/litres_corrected", payload=corrected_value, qos=0, retain=False)


client = mqtt.Client()  # create new instance

client.on_connect = on_connect
client.on_message = on_message

client.connect(broker_address, 1883, 60)  # connect to broker
# client.publish("house/main-light", "OFF")  # publish
client.loop_forever()
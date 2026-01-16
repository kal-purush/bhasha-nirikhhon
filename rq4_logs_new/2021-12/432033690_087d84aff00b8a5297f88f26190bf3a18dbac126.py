import paho.mqtt.client as mqtt

topic = "deviceid/team3/evt/angle"
server = "3.84.34.84"

def on_connect(client, userdata, flags, rc):
    print("Connected with RC : " + str(rc))
    client.subscribe(topic)

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload.decode('UTF-8')))
    if int(msg.payload.decode('UTF-8')) == 1:
        client.publish("deviceid/team3_b/cmd/angle_b", "1")
    elif int(msg.payload.decode('UTF-8')) == 2:
        client.publish("deviceid/team3_b/cmd/angle_b", "2")
    elif int(msg.payload.decode('UTF-8')) == 3:
        client.publish("deviceid/team3_b/cmd/angle_b", "3")
    elif int(msg.payload.decode('UTF-8')) == 4:
        client.publish("deviceid/team3_b/cmd/angle_b", "4")
    elif int(msg.payload.decode('UTF-8')) == 5:
        client.publish("deviceid/team3_b/cmd/angle_b", "5")
    elif int(msg.payload.decode('UTF-8')) == 6:
        client.publish("deviceid/team3_b/cmd/angle_b", "6")
    elif int(msg.payload.decode('UTF-8')) == 0:
        client.publish("deviceid/team3_b/cmd/angle_b", "0")
    

client = mqtt.Client()
client.connect(server, 1883, 60)
client.on_connect = on_connect
client.on_message = on_message

client.loop_forever()
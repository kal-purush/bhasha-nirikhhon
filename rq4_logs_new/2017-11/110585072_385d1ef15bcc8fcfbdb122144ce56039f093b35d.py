import time
from ds18b20 import ds18b20
import ener
import mosquitto as mqtt

power = ener.Energenie()
is_connected = False
is_first = True
brew = [{"heater":False,"min":20,"max":21}, {"heater":False,"min":20,"max":21}]

def mqtt_connect(client, userdata, rc):
  global is_connected
  global is_first
  print("Connected with result code {}".format(rc))
  client.subscribe("beer/brew1/heater", 2)
  client.subscribe("beer/brew1/min_temp", 2)
  client.subscribe("beer/brew1/max_temp", 2)
  client.subscribe("beer/brew2/heater", 2)
  client.subscribe("beer/brew2/min_temp", 2)
  client.subscribe("beer/brew2/max_temp", 2)
  is_connected = True
  is_first = True 

def mqtt_disconnect(client, userdata, rc):
  global is_connected
  is_connected = False
  
def on_message(client, userdata, msg):
  global brew

  if msg.topic[:9] == "beer/brew":
    try:
      brewnum = int(msg.topic[9])
      attr = (msg.topic.split("/")[:1:-1])[0]
      if attr == "heater":
        brew[brewnum-1]["heater"] = (msg.payload == "on")
        power.switch(brewnum, brew[brewnum-1]["heater"])
      elif attr == "min_temp":
        brew[brewnum-1]["min"] = float(msg.payload)
      elif attr == "max_temp":
        brew[brewnum-1]["max"] = float(msg.payload)
    except ValueError:
      pass
    except EnerError:
      pass
    
try:
  sensors = [ds18b20("28-0517022710ff"), ds18b20("28-041702ce85ff")]
  client= mqtt.Mosquitto()
  client.on_connect = mqtt_connect
  client.on_message = on_message
  client.on_disconnect = mqtt_disconnect
  client.will_set("beer/server", "offline", 2, True)
  client.connect_async("192.168.1.200")
  client.loop_start()

  while 1:
    if (is_connected):
      if (is_first):
        client.publish("beer/server", "online", 2, True)
        is_first = False

      # Iterate through sensors
      for s in range(len(sensors)):
        temp = sensors[s].temperature
        client.publish("beer/brew{0}/temperature".format(s+1), temp)

        # Check temperature. Set heater
        if temp != -273.0:
          try:
            if brew[s]["min"] > temp:
              brew[s]["heater"] = True
              client.publish("beer/brew{0}/heater".format(s+1), "on", 2, True)
            elif brew[s]["max"] < temp:
              brew[s]["heater"] = False
              client.publish("beer/brew{0}/heater".format(s+1), "off", 2, True)
          except KeyError:
            pass
          except IndexError:
            pass

      # Control heater. Keep sending the signal since it isn't reliable
      for i in range(len(brew)):
        try:
          power.switch(i+1, brew[i]["heater"])
        except EnerError:
          pass

      
    time.sleep(5)
 
except KeyboardInterrupt:
  power.cleanup()
  client.loop_stop()
  if (is_connected):
    client.disconnect()
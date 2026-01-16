import json
import os

with open('./new.json') as f:
  data = json.load(f)

failed = []

for key in data["Dependencies"]:
	# print(key)
	s = "pip install " + key +  "==" + data["Dependencies"][key]
	print(s)
	if os.system(s) != 0:
		failed.append(key)

for mod in failed:
	print(mod, "could not be installed")
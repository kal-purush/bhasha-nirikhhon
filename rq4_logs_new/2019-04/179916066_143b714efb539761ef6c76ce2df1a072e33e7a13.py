import json

handle = open("complex.json", "r")
read = handle.read()

d = json.loads(read)

d[2]["batters"]["batter"][1]["type"] = "Ice-Cream"
print(d)
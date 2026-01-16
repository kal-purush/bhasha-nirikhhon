import json

# car_data = {
#     "name": "Tesla",
#     "type": "Electric",
#     "faults": 9384,
#     "death trap": True
# }
#
# # print(car_data, type(car_data))
# #
# # print(car_data["faults"])
#
# dumps = json.dumps(car_data)
#
# print(dumps, type(dumps))
#
# with open("tesla.json", "w") as jsonfile:
#     json.dump(car_data, jsonfile)
#
#

with open("spartan.json") as jsonfile:
    spartan = json.load(jsonfile)

print(spartan, type(spartan))
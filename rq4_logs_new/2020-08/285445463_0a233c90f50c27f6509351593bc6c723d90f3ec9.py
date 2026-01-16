from resources.info import Info
import json

info = Info()
# print(info.courses["javascript"])


def walk_json(json, last=""):
    output = {"intents":[]}
    for key in json:
        if isinstance(json[key], str):
            output["intents"].append(
                {
                    "pattern": f"{last} {key}",
                    "responses": [json[key]],
                    "tag": f"{last} {json[key]}",
                }
            )
        elif isinstance(json[key], list):
            # print("found a list")
            print(
                {
                    "pattern": f"{last} {key}",
                    "responses": json[key],
                    "tag": f"{last} {json[key]}",
                }
            )
        elif hasattr(json[key], "__iter__"):
            return walk_json(json[key], f"{last} {key}")
        else:
            print(last, json[key])
    return output


intents = walk_json(info.courses)
print(json.dumps(intents))
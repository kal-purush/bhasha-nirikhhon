import json
from json import JSONEncoder

class AdaFruitConfig:
    def __init__(self, clientId, adafruitId, adafruitKey, brokerAddress, pubTopic):
        self.clientId = clientId
        self.adafruitId = adafruitId
        self.adafruitKey = adafruitKey
        self.brokerAddress = brokerAddress
        self.pubTopic = pubTopic

class AppConfiguration:
    def __init__(self, AdaFruitConfig):
        self.AdaFruitConfig = AdaFruitConfig

class AppConfigurationEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__

def parse_app_adafruit_config(file_path):
    try:
        with open(file_path, "r") as file:
            appJson = file.read()

        appDict = json.loads(appJson)

        if "adaFruitConfig" in appDict:
            adaFruitConfig = AdaFruitConfig(**appDict["adaFruitConfig"])
            appConfig = AppConfiguration(AdaFruitConfig=adaFruitConfig)
            return appConfig
        else:
            raise KeyError("adaFruitConfig not found in the JSON")

    except FileNotFoundError as e:
        print(f"File '{file_path}' not found.")
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON file: {e}")
    except PermissionError as e:
        print(f"Permission denied: {e}")
    except KeyError as e:
        print(f"Key error in JSON file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
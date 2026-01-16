import pymongo
import os
from dotenv import load_dotenv
load_dotenv()

db_client = pymongo.MongoClient(os.getenv('mongo_string'))
database = db_client.get_database("CodingChallengeBot")
collection = database.get_collection("Challenges")

for doc in collection.find({}):
    if ("code" in doc):
        if "python" in doc["code"]:
            if ("preloaded" in doc["code"]["python"]["exampleFixture"]):
                print("replacing")
                string:str = doc["code"]["python"]["exampleFixture"]
                string = string.replace("import preloaded\n", "")
                doc["code"]["python"]["exampleFixture"] = string
                collection.replace_one({"_id": doc["_id"]}, doc)
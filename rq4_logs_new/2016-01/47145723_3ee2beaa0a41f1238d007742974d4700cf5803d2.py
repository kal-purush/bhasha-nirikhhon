#!/usr/bin/env python3

from pprint import pprint

from gcm import GCM
from pymongo import MongoClient

from config import *


gcm = GCM(GCM_API_KEY)
data = {
        "event": "YoutubeSongAdded",
        "_id": "FGBhQbmPwH8",
        "playlist": "PLB5VrND_o3PgZNzNdohFDWE5BTFIPDImQ",
        "title": "Daft Punk - One More Time",
        "type": "Youtube",
        "url": "http://s3.storage.ms.wut.ee/FGBhQbmPwH8"
    }


client = MongoClient()
db = client["MusicSync"]

reg_ids = list( token["_id"] for token in db["gcm"].find())

response = gcm.json_request(registration_ids=reg_ids, data=data)
pprint(response)
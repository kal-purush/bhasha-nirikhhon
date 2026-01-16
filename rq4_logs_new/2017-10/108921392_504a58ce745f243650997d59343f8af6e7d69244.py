import pprint
import json
import datetime
import os
import sys
import getopt
import codecs
import requests
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.EDCA

recent = db.Recent
releases = db.Releases
"""
# obten OCIDs recientemente actualizados de SHCP y SFP
# e insertalos en una colección sin repetidos

# recent.insert_one(...)


# por cada ocid en Recent, borrar releases correspondientes en Releases,
# obtener los releases correspondientes de ambos lados e insertarlos en Releases
for r in recent.aggregate([{ "$group": { "_id": "$ocid", "conteo": { "$sum": 1 }}}]):
    releases.remove({ "ocid": r["ocid"]})

    # Get releases from SHCP and SFP
    
    rs = []
    collection.insert(rs)
"""
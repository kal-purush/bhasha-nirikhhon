""" Flask configuration to connect to the database """
from flask_pymongo import pymongo
import os

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")


client = pymongo.MongoClient(f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@dbexample.kadqv.mongodb.net/{DB_NAME}?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE")

# nombre de la db
db = client.iot
from pymongo import MongoClient

DOMAIN = "0.0.0.0"
PORT = 27017

client = MongoClient(host=[str(DOMAIN) + ":" + str(PORT)],
                     serverSelectionTimeoutMS=3000,
                     username='root',
                     password='example')

orders_database = client['orders']
from pymongo import MongoClient
import datetime


client = MongoClient('localhost', 27017)

db = client.test_database

collection = db.test_collection

post = {
    "author":"Mike",
    "text":"Mongodb post",
    "date":datetime.datetime.utcnow()}

posts = db.posts
pid = posts.insert(post)
print (pid)

print (db.collection_names())

print (posts.find_one())
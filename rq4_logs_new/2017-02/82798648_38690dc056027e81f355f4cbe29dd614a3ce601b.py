import json, pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.dataBot

def dbconnection():
    collection = db.user

    fname = input("What is your name?")
    lname = input("What is your lastname")
    sq = input("give me a secret")
    aq = input("the answer to that secret is:")

    post = {"fName": fname,
            "lName": lname,
            "sQ": sq,
            "aQ": aq}

    posts = db.user
    post_id = posts.insert_one(post).inserted_id

def dbquestion():
    collection = db.questions

    question = input("What is your question? ")

    post = {"question": question}

    posts = db.questions
    post_id = posts.insert_one(post).inserted_id
import requests
from .settings import settings
from .db import Db
import time

if __name__ != '__main__':
    users_db = Db.get_users_db()

def register(o):

    if "email_addr" in o and not users_db.find_one({"connections.email.email_addr": o["email_addr"]}):
        users_db.insert_one({
            "connections": {
                "email": {
                    "email_addr": o["email_addr"],
                    "name": o["name"],
                }
            }
        })


def send(email_addr, subject, content):
    data = users_db.find_one({"connections.email.email_addr": email_addr})
    print(f'send mail to {email_addr}')
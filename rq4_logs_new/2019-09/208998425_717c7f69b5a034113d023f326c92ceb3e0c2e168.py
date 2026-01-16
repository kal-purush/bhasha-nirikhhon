from . import scheduler
from .db import get_collection
import time
from bson.objectid import ObjectId
from . import util

certificate = get_collection('certificate')


def generate_token(real_user_id, expire_in=3600):
    certificate.delete_many({"real_user_id": real_user_id})
    while True:
        token = util.generate_id(50)
        if not certificate.find_one({"token": token}):
            break
    certificate.insert_one({"real_user_id": real_user_id, "token": token, "expire_at": time.time()+expire_in})
    return real_user_id, token


def validate_token(real_user_id, token, expire=True):
    data = certificate.find_one({"token": token})
    if expire and data:
        certificate.delete_one({"token": token})
    return data and data["real_user_id"] == real_user_id


def delete_expired():
    certificate.delete_many({"expire_at": {"$lt": time.time()}})


scheduler.add_task(60, delete_expired)
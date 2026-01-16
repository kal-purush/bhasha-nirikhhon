from . import task
from .db import Db
import time
from bson.objectid import ObjectId
from . import util

upload = Db.get_upload_db()


def generate_upload_link(real_user_id):
    print(real_user_id, type(real_user_id))
    while True:
        token = util.generate_id(50)
        if not upload.find_one({"token": token}):
            break
    upload.insert_one({"real_user_id": real_user_id, "token": token, "create_time": time.time()})
    return f'https://kyukou.shosato.jp/c/uploadcsv/?token={token}&realid={real_user_id}'


def validate_upload_token(real_user_id, token):
    data = upload.find_one({"token": token})
    if data:
        upload.delete_one({"token": token})
    return data and data["real_user_id"] == real_user_id


def delete_expired(expire_sec=3600):
    upload.delete_many({"create_time": {"$lt": time.time()-expire_sec}})


task.add_task(delete_expired, 60)
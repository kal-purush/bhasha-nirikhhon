from pprint import pprint
from pymongo import MongoClient
import datetime

from .settings import settings


class Db:
    @classmethod
    def init(cls, url):
        cls.client = MongoClient(url)
        cls.db = cls.client.kyukou
        cls.users = cls.db.users
        cls.lectures = cls.db.lectures


Db.init(settings.url)
Db.lectures.insert_one({"id": "",
                        "periods": [1, 2],
                        "dayofweek": 2,
                        "date": None,
                        "teachers": ["佐藤"]
                        })
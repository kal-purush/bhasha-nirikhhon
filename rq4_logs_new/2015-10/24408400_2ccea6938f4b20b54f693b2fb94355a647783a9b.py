from datetime import datetime
import pymongo
import tornado.gen

import settings


class Result(object):
    collection = settings.db['results']

    def __init__(self, **kwargs):
        self._id = kwargs.get('_id')
        self.login = kwargs.get('login', 'unknown')
        self.score = kwargs.get('score', 0)
        self.date = datetime.now()

    @property
    def result_id(self):
        return str(self._id) if self._id else None

    @classmethod
    @tornado.gen.coroutine
    def get_all(cls):
        results = {}
        cursor = Result.collection.find()
        while (yield cursor.fetch_next):
            result = cls(**cursor.next_object())
            results[result.result_id] = result
        return results

    def __lt__(self, other):
        return self.score > other.score

    @classmethod
    @tornado.gen.coroutine
    def count(cls):
        return Result.collection.count()

    @classmethod
    def get_last(cls):
        last = Result.collection.find().sort(
            'score', pymongo.DESCENDING
        ).limit(1)

        return last

    def put(self):
        data = {
            'login': self.login,
            'score': self.score,
            'date': self.date
        }

        return Result.collection.save(data)
# -*- coding: utf-8 -*-

import datetime
from peewee import *

db = SqliteDatabase('data.db')


# 文章表
class Article(Model):
    _id = PrimaryKeyField
    postId = CharField(unique=True)  # url md5后的值
    nickname = CharField()  # 公众号
    title = CharField()
    url = CharField()
    content = TextField(null=True)
    state = CharField(default=0)
    createdTime = DateTimeField(default=datetime.datetime.now)
    grabState = IntegerField(default=0, null=False)  # 抓取状态
    timestamp = DateTimeField(null=True, default=datetime.datetime.now)

    class Meta:
        database = db


# 图片列表
class Image(Model):
    _id = PrimaryKeyField
    postId = CharField()
    image_url = CharField(unique=True)
    createdTime = DateTimeField(default=datetime.datetime.now)
    grabState = IntegerField(default=0, null=False)  # 抓取状态
    timestamp = DateTimeField(null=True, default=datetime.datetime.now)

    class Meta:
        database = db


class Info(Model):
    count = IntegerField(default=0, null=False)  # 文章总数
    pages = IntegerField(default=0, null=False)  # 文章总页数
    grabCount = IntegerField(default=0, null=False)  # 抓取条数
    createdTime = DateTimeField(default=datetime.datetime.now)
    timestamp = DateTimeField(null=True, default=datetime.datetime.now)

    class Meta:
        database = db


def delete_table():
    return Article.delete().execute()


def init_table():
    db.connect()
    db.create_tables([Article, Image, Info])


if __name__ == '__main__':
    init_table()
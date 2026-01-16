from fastapi import FastAPI
from peewee import *
import time
import random
import string
import datetime
import os
from playhouse.db_url import connect

# PostgreSQL database
db = connect(os.environ.get('DATABASE_URL'))

# Peewee model
class BaseModel(Model):
    class Meta:
        database = db

class User(BaseModel):
    name = CharField()
    age = IntegerField()
    created_at = DateTimeField(default=datetime.datetime.now)

# FastAPI app
app = FastAPI()

# Helper to measure time
def timing(func):
    async def wrapper(*args, **kwargs):
        start = time.time()
        result = await func(*args, **kwargs)
        duration = time.time() - start
        return {
            "duration_seconds": round(duration, 4),
            "result": result
        }
    return wrapper

@app.post("/create_table")
@timing
async def create_table():
    db.connect(reuse_if_open=True)
    db.create_tables([User], safe=True)
    return "Table created"

@app.post("/insert/{n}")
@timing
async def insert_n(n: int):
    db.connect(reuse_if_open=True)

    with db.atomic():
        for _ in range(n):
            User.create(
                name="".join(random.choices(string.ascii_letters, k=10)),
                age=random.randint(18, 80)
            )
    return f"Inserted {n} users"

@app.get("/get_all")
@timing
async def get_all():
    db.connect(reuse_if_open=True)
    users = list(User.select().dicts())
    return {
        "total_users": len(users),
        "users": users[:10]  # only return first 10 for brevity
    }
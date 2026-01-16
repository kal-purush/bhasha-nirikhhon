from models import TableUser
from passlib.hash import argon2
import sqlalchemy as sa


async def hash_password(password):
    pass_hash = argon2.hash(password)
    return pass_hash


async def check_user(db, username):
    async with db.acquire() as conn:
        s = sa.select([TableUser]).where(TableUser.username == username)
        return await conn.execute(s)


async def create_user(db, username, pass_hash, name, age):
    user = await check_user(db, username)
    if not user.rowcount:
        async with db.acquire() as conn:
            await conn.execute(
                sa.insert(TableUser).values(
                    username=username,
                    password=pass_hash,
                    name=name,
                    age=age,
                )
            )
        return True
    return False


async def find_user(db, user):
    async with db.acquire() as conn:
        s = sa.select([TableUser]).where(TableUser.username == user)
        res = await conn.execute(s)
        return await res.fetchone()


async def login_user(db, username, password):
    row = await find_user(db, username)
    if not row:
        return False
    pass_in_db = row[2]
    if argon2.verify(password, pass_in_db):
        return True
    return False


async def user_info(db, username):
    row = await find_user(db, username)
    id = row[0]
    username = row[1]
    name = row[3]
    age = row[4]
    return [id, username, name, age]
from sqlalchemy import insert, select

from src.auth.models import role
from tests.conftest import client, async_session_maker


async def test_add_role():
    async with async_session_maker() as session: # Подключаемся к БД
        stmt = insert(role).values(id=1, name="Admin", permission=None)
        await session.execute(stmt)
        await session.commit()

        query = select(role)
        result = await session.execute(query)
        print(result.all())


# def test_register():
#     client.post("/auth/register", json={
#         "email": "string",
#         "password": "string",
#         "is_active": True,
#         "is_superuser": False,
#         "is_verified": False,
#         "username": "string",
#         "role_id": 1
#     })
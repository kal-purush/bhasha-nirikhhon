from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

engine = create_async_engine("sqlite+aiosqlite:///sql_app.db")
new_session = async_sessionmaker(engine, expire_on_commit=False)


class Model(DeclarativeBase):
    pass


class EmployeeOrm(Model):
    __tablename__ = "employee"

    id: Mapped[int] = mapped_column(primary_key=True)
    name_and_surname: Mapped[str]
    age: Mapped[int]
    position: Mapped[str]
    remote: Mapped[bool]


async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Model.metadata.create_all)


async def delete_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Model.metadata.drop_all)
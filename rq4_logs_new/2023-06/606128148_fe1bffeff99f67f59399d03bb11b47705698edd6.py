from copy import deepcopy
from typing import NoReturn
from uuid import UUID

from sqlalchemy import TIMESTAMP
from sqlalchemy import UUID as SQLUUID
from sqlalchemy import Boolean, Column, MetaData, String, Table, insert, select

from src.config.database import engine
from src.core.entity.account import Account, AccountType
from src.core.usecase.driven.creating_account import CreatingAccount
from src.core.usecase.driven.reading_account import ReadingAccount
from src.core.usecase.driven.update_account import UpdateAccount

metadata_obj = MetaData()

account_table = Table(
    "account",
    metadata_obj,
    Column("id", SQLUUID, nullable=False),
    Column("external_id", SQLUUID, nullable=False),
    Column("type", String(20), nullable=False),
    Column("is_enable", Boolean),
    Column("created_at", TIMESTAMP),
)


class AccountRepository(CreatingAccount, ReadingAccount, UpdateAccount):
    def create(self, external_id: UUID, type: AccountType) -> Account:
        with engine.connect() as conn:
            insert_line = (
                insert(account_table)
                .values(external_id=external_id, type=type.name)
                .returning(account_table.c.id, account_table.c.created_at)
            )
            cursor = conn.execute(insert_line)
            (id, created_at) = deepcopy(cursor.first())
            conn.commit()
            return Account(id, external_id, type, True, created_at)

    def by_id(self, account_id: UUID) -> Account:
        with engine.connect() as conn:
            query = (
                select(account_table).where(account_table.c.id == account_id).limit(1)
            )
            cursor = conn.execute(query)
            (id, external_id, type, is_enable, created_at) = deepcopy(cursor.first())
            return Account(id, external_id, type, is_enable, created_at)

    def change_type(self, account_id: UUID, type: AccountType) -> NoReturn:
        pass
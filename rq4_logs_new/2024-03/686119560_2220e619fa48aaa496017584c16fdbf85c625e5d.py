import peewee

import helper
from models.base import BaseModel
from models.account import Account


class UnsignedIntegerField(peewee.IntegerField):
    field_type = 'int unsigned'


# noinspection PyTypeChecker
class Assets(BaseModel):
    user = peewee.ForeignKeyField(Account, to_field='user_id', backref='inventory', primary_key=True)
    cash = UnsignedIntegerField(default=0)
    gold = UnsignedIntegerField(default=0)

    @staticmethod
    async def fetch(user_id: str):
        inventory, is_created = Assets.get_or_create(user_id=user_id)
        return inventory

    @staticmethod
    async def update_assets(user_id=None, user=None, **kwargs):
        expected_args = ['cash', 'cash_delta', 'gold', 'gold_delta']
        user = user or await Assets.fetch(user_id=user_id)

        for key, value in kwargs.items():
            if key not in expected_args:
                raise AttributeError("Tried updating account with bad argument")

            if value is not None:
                if value == "NULL":
                    value = None

                # Parameters ending in _delta are meant to modify without overwriting existing data
                if key[-6:] == '_delta':
                    curr_data = getattr(user, key[:-6])
                    setattr(user, key[:-6], curr_data + value)
                else:
                    setattr(user, key, value)
        user.save()

    @staticmethod
    async def leaderboard(num_users: int):
        return [user.user_id for user in Assets.select().order_by(-Assets.cash)][:num_users]

    def fmoney(self) -> str:
        return helper.to_dollars(int(self.cash))
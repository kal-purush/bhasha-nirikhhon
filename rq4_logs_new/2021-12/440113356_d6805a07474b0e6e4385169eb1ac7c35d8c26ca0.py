from tortoise import fields
from tortoise.models import Model


class Bot(Model):
    id = fields.BigIntField(pk=True)
    avatar = fields.CharField(max_length=100)
    # start_date = fields.DatetimeField(null=True)
    # expiry_date = fields.DatetimeField(null=True)
    # node = fields.ForeignKeyField("models.Node", related_name="node")
    # confirmed = fields.BooleanField(default=False)
    # logplex_url = fields.TextField(null=True)
    # last_log_timestamp = fields.CharField(max_length=30, null=True)
    # current_build_id = fields.CharField(max_length=45, null=True)

    class Meta:
        table = "main_site_bot"
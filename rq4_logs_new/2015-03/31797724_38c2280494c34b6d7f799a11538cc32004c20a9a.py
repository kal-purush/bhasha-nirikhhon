from sqlalchemy import *
from migrate import *


from migrate.changeset import schema
pre_meta = MetaData()
post_meta = MetaData()
articles = Table('articles', post_meta,
    Column('id', Integer, primary_key=True, nullable=False),
    Column('content', String(length=10000), nullable=False, default=ColumnDefault('')),
    Column('title', String(length=200), nullable=False, default=ColumnDefault('')),
    Column('category', String(length=100), nullable=False, default=ColumnDefault('')),
    Column('publish_date', DateTime),
    Column('is_cook', Integer, nullable=False, default=ColumnDefault('')),
    Column('is_nikkei', Integer, nullable=False, default=ColumnDefault('')),
)


def upgrade(migrate_engine):
    # Upgrade operations go here. Don't create your own engine; bind
    # migrate_engine to your metadata
    pre_meta.bind = migrate_engine
    post_meta.bind = migrate_engine
    post_meta.tables['articles'].create()


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    pre_meta.bind = migrate_engine
    post_meta.bind = migrate_engine
    post_meta.tables['articles'].drop()
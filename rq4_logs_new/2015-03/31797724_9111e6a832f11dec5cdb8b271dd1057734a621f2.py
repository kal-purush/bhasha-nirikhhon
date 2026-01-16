from sqlalchemy import *
from migrate import *


from migrate.changeset import schema
pre_meta = MetaData()
post_meta = MetaData()
articles = Table('articles', pre_meta,
    Column('id', Integer, primary_key=True, nullable=False),
    Column('content', Text, nullable=False),
    Column('title', String, nullable=False),
    Column('category', String, nullable=False),
    Column('publish_date', DateTime),
    Column('is_cook', Integer, nullable=False),
    Column('is_nikkei', Integer, nullable=False),
)


def upgrade(migrate_engine):
    # Upgrade operations go here. Don't create your own engine; bind
    # migrate_engine to your metadata
    pre_meta.bind = migrate_engine
    post_meta.bind = migrate_engine
    pre_meta.tables['articles'].columns['category'].drop()


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    pre_meta.bind = migrate_engine
    post_meta.bind = migrate_engine
    pre_meta.tables['articles'].columns['category'].create()
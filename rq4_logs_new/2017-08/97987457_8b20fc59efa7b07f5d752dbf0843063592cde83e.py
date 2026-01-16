from sqlalchemy import *
from migrate import *


from migrate.changeset import schema
pre_meta = MetaData()
post_meta = MetaData()
client_appt = Table('client_appt', post_meta,
    Column('id', INTEGER, primary_key=True, nullable=False),
    Column('client_id', INTEGER),
    Column('therapist_id', INTEGER),
    Column('start_datetime', DATETIME),
    Column('end_datetime', DATETIME),
    Column('appointment_type', VARCHAR(length=15)),
    Column('appt_type_id', INTEGER),
    Column('cancelled', SMALLINT, default=ColumnDefault(0)),
    Column('location', VARCHAR(length=255)),
    Column('mileage', INTEGER, default=ColumnDefault(0)),
    Column('billing_xml_id', INTEGER),
)


def upgrade(migrate_engine):
    # Upgrade operations go here. Don't create your own engine; bind
    # migrate_engine to your metadata
    pre_meta.bind = migrate_engine
    post_meta.bind = migrate_engine
    post_meta.tables['client_appt'].columns['location'].create()


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    pre_meta.bind = migrate_engine
    post_meta.bind = migrate_engine
    post_meta.tables['client_appt'].columns['location'].drop()
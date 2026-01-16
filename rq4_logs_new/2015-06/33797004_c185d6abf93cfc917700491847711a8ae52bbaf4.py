from sqlalchemy import MetaData,create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session,mapper
metadata=MetaData()
engine = create_engine('postgresql://postgres:minkovski@postgres.d/MusicGenreMigration')
for table in engine.table_names():
    print (table)
metadata.reflect(engine, only=['dicband','dicsong','userlist','listening','dicgenre','bandgenremx'])
print('--------------')
print(engine.table_names()[5])
print('--------------')
Base = automap_base(metadata=metadata)
Base.prepare()
Band=Base.classes.dicband
Song=Base.classes.dicsong
Band=Base.classes.dicband
Song=Base.classes.dicsong
User=Base.classes.userlist
Listening=Base.classes.listening
Genre=Base.classes.dicgenre
BandAndGen=Base.classes.bandgenremx

session = Session(engine)
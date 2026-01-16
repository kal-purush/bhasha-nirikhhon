from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#SQLALCHEMY_DATABASE_URL = "sqlite:///./moody.db"

db_url='database-1.cqooqygygpon.us-east-2.rds.amazonaws.com'
db_name='postgres'
db_username='moodydb'
db_password='Group10SW'

SQLALCHEMY_DATABASE_URL = f'postgresql://{db_username}:{db_password}@{db_url}/{db_name}'

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={}
    
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
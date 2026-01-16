# Databaseとの接続を行う
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os

# dbファイルパスを定義
database_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "flareg.db")
# SQLiteを利用してDBを構築
engine = create_engine("sqlite:///" + database_file, convert_unicode=True)
# DB接続用インスタンスを生成
db_session = scoped_session(sessionmaker(autocommit=False,autoflush=False,bind=engine))
# Baseオブジェクトを生成して、DBの情報を入れる
Base = declarative_base()
Base.query = db_session.query_property()

def init_db():
    import models.models
    Base.metadata.create_all(bind=engine)
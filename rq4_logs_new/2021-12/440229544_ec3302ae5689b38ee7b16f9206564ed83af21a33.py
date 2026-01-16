from sqlalchemy import create_engine, MetaData

# engine = create_engine("mysql+pymysql://{user_name}:{user_password}@localhost:{port}/{database_name}")
meta = MetaData()
connection = engine.connect()
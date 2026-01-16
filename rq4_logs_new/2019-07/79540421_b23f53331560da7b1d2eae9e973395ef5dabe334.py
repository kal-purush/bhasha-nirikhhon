from app.models import Collectionrecord , Defaulter
from sqlalchemy.sql import func
from app import db
import pandas as pd

q= Collectionrecord.query.join(Collectionrecord.defaulters).group_by(Defaulter.admno).all()
print(q)
#r=db.select([func.count(Collectionrecord.id)])
#print(r)
result = db.engine.execute(text("select * from collectionrecord").execution_options(autocommit=True)).fetchall()
df = pd.DataFrame(result)
print(df)
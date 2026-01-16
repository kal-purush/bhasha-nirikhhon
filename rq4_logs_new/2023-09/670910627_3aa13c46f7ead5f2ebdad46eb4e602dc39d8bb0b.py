from sqlalchemy.orm import base
from sqlalchemy.engine import create_engine
from sqlalchemy import column ,String


base =declerative_base()

class Article(Base):
    __tablename__='articles'
    id=column(Integer,primary_key=True)
    title=column(String)
    author=column(String)
    pub_data=column(String)
    summary=column(String)


    def __str__(self):
        return self.title
    
    
if __name__=="__main__":
        engine = create_engine('sqllite:///srticle.db', echo=True)
        base.metadata.create_all(engine)
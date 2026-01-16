from sqlalchemy import Column, Integer, String, Text

from database.sqlite import Base


class MetaTraining(Base):
    __tablename__ = "meta_training"

    id = Column(Integer, primary_key=True)
    task = Column(String, nullable=False)
    instruct = Column(Text, nullable=False)
    memory = Column(Text, nullable=True)
    input = Column(Text, nullable=False)
    output = Column(Text, nullable=False)
    
# 1.    alembic init migrations

# 2.    [파일명: projects/myapi/alembic.ini]
#       (... 생략 ...)
#       sqlalchemy.url = sqlite:///./myapi.db
#       (... 생략 ...)

# 3.    [파일명: projects/myapi/migrations/env.py]
#       (... 생략 ...)
#       import models
#       (... 생략 ...)
#       target_metadata = models.Base.metadata
#       (... 생략 ...)

# 4.    alembic revision --autogenerate

# 5.    alembic upgrade head


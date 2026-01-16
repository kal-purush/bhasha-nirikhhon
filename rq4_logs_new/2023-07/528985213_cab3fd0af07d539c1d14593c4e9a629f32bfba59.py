import logging
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker


logger = logging.getLogger(__name__)

engine = create_engine("sqlite:///plans.db", isolation_level="AUTOCOMMIT")
base = declarative_base()
base.metadata.bind = engine
base.metadata.create_all(engine)


@contextmanager
def db_session():
    session = scoped_session(
        sessionmaker(
            bind=engine, autoflush=True, autocommit=False, expire_on_commit=True
        )
    )
    try:
        yield session
    except Exception as e:
        logger.exception(e)
        session.rollback()
        raise
    finally:
        session.close()
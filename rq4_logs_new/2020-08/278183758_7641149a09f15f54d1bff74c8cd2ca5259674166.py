from starlette.config import Config
from starlette.datastructures import Secret
from sqlalchemy.engine.url import make_url, URL
from dotenv import load_dotenv

load_dotenv()

config = Config(".env")


DB_DRIVER = config("DB_DRIVER", default="postgresql")
DB_HOST = config("HOST", default=None)
DB_PORT = config("PORT", cast=int, default=None)
DB_USER = config("USER", default=None)
DB_PASSWORD = config("PASSWORD", cast=Secret, default=None)
DB_DATABASE = config("DATABASE", default=None)
DB_DSN = config(
    "DB_DSN",
    cast=make_url,
    default=URL(
        drivername=DB_DRIVER,
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_DATABASE,
    ),
)
DB_POOL_MIN_SIZE = config("MINSIZE", cast=int, default=1)
DB_POOL_MAX_SIZE = config("MAXSIZE", cast=int, default=16)
DB_ECHO = config("ECHO", cast=bool, default=False)

BROKER_CONN_URI = config("BROKER_CONN_URI")
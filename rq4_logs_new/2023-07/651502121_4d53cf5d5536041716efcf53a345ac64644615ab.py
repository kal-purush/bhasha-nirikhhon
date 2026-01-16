from dotenv import load_dotenv
from os import getenv
from scrapy.utils.log import configure_logging
from logging import basicConfig, INFO


configure_logging(install_root_handler=False)
basicConfig(
    filename="logs.log",
    format="[%(asctime)s] %(name)s %(levelname)s: %(message)s",
    level=INFO,
)

load_dotenv()

POSTGRES_PORT = getenv("POSTGRES_PORT") or 5432
POSTGRES_DB = getenv("POSTGRES_DB") or "postgres"
POSTGRES_USER = getenv("POSTGRES_USER") or "postgres"
POSTGRES_PASSWORD = getenv("POSTGRES_PASSWORD") or "password"
POSTGRES_HOST = getenv("POSTGRES_HOST") or "127.0.0.1"

RABBITMQ_DEFAULT_USER = getenv("RABBITMQ_DEFAULT_USER") or "user"
RABBITMQ_DEFAULT_PASS = getenv("RABBITMQ_DEFAULT_PASS") or "password"
RABBITMQ_DEFAULT_HOST = getenv("RABBITMQ_DEFAULT_HOST") or "localhost"
RABBITMQ_MAIN_QUEUE = getenv("RABBITMQ_MAIN_QUEUE") or "scrapy-soup"
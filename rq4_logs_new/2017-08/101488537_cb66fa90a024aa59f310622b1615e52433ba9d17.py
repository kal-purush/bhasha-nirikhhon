# Statement for enabling the development environment
# Enabling the development environment
DEBUG = True
DB_USER = 'postgres'
DB_PASS = 'postgres'
DB_HOST = 'localhost'
# DB_PORT = '80'
DB_NAME = 'my3r'

# Define the application directory
import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Define the database - we are working with
# SQLite for this example
SQLALCHEMY_DATABASE_URI = 'postgres://' + DB_USER + ':' + DB_PASS + '@' + DB_HOST + '/' + DB_NAME
DATABASE_CONNECT_OPTIONS = {}

# Application threads. A common general assumption is
# using 2 per available processor cores - to handle
# incoming requests using one and performing background
# operations using the other.
THREADS_PER_PAGE = 2

# Enable protection agains *Cross-site Request Forgery (CSRF)*
CSRF_ENABLED = True

# Use a secure, unique and absolutely secret key for
# signing the data.
CSRF_SESSION_KEY = "secret"

# Secret key for signing cookies
SECRET_KEY = "thesecretsss"

LOGGING_FORMAT = '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
LOGGING_LOCATION = 'logs/error.log'
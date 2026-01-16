__author__ = 'jiaying.lu'

__all__ = ["LOGENTRIES_TOKEN", "BUGSNAG_TOKEN", "APP_ENV"]
import os

# Settings

LOGENTRIES_DEV_TOKEN = "768ebdbc-b889-4a84-9850-f7eecf728aa3"
LOGENTRIES_PROD_TOKEN = "27274a9e-8e2f-4e08-ba29-1bbb46c5047d"
LOGENTRIES_LOCAL_TOKEN = "768ebdbc-b889-4a84-9850-f7eecf728aa3"

BUGSNAG_DEV_TOKEN = "eed4e480fb8fba0597eef2833d45dd4f"
BUGSNAG_PROD_TOKEN = "ccf314cc898d9edeccd67b01b8b79815"
BUGSNAG_LOCAL_TOKEN = "eed4e480fb8fba0597eef2833d45dd4f"

LOGENTRIES_TOKEN = ""
BUGSNAG_TOKEN = ""
APP_ENV = ""

# Configuration

try:
    APP_ENV = os.environ["APP_ENV"]
except KeyError, key:
    print "KeyError: There is no env var named %s" % key
    print "The local env will be applied"
    APP_ENV = "local"
finally:
    if APP_ENV == "dev":
        LOGENTRIES_TOKEN = LOGENTRIES_DEV_TOKEN
        BUGSNAG_TOKEN = BUGSNAG_DEV_TOKEN
    elif APP_ENV == "prod":
        LOGENTRIES_TOKEN = LOGENTRIES_PROD_TOKEN
        BUGSNAG_TOKEN = BUGSNAG_PROD_TOKEN
    elif APP_ENV == "local":
        LOGENTRIES_TOKEN = LOGENTRIES_LOCAL_TOKEN
        BUGSNAG_TOKEN = BUGSNAG_LOCAL_TOKEN
import os
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='[%(levelname)8s] %(process)s:%(name)s\t%(message)s')
log = logging.getLogger(__name__)


"""
SECRETS & CONFIG
=================
The following values come from your .env file, and either pertain to
your app's configuration (i.e., API_BASE_URL) or are secrets that may
have different values for each environment (staging, prod)

You'll get warnings if any of these are unset
"""


GITHUB_USER = os.getenv("GITHUB_USER", -1)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", -1)
API_BASE_URL = os.getenv("API_BASE_URL", -1)


def validate_env_variables():
    if (GITHUB_USER == -1 or GITHUB_TOKEN.count == 0):
        log.warn("GITHUB_USER env variable has not yet been set")
    if (GITHUB_TOKEN == -1 or GITHUB_TOKEN.count == 0):
        log.warn("GITHUB_TOKEN env variable has not yet been set")
    if (API_BASE_URL == -1 or API_BASE_URL.count == 0):
        log.warn("API_BASE_URL env variable has not yet been set")
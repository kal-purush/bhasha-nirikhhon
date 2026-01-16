import json

from . import DATADIR

JWKS_TEST_FILENAME = str(DATADIR / "samtrafiken-test.json")
JWKS_PROD_FILENAME = str(DATADIR / "samtrafiken-prod.json")


ENVIRONMENTS = {
    "test": {
        "keys_filename": JWKS_TEST_FILENAME,
        "jwks": json.load(open(JWKS_TEST_FILENAME)),
        "metadata_url": "https://bobmetadata-pp.samtrafiken.se/api/v2/participantMetadata",
    },
    "prod": {
        "keys_filename": JWKS_PROD_FILENAME,
        "jwks": json.load(open(JWKS_PROD_FILENAME)),
        "metadata_url": "https://bobmetadata.samtrafiken.se/api/v2/participantMetadata",
    },
}


def where(env: str = "prod") -> str:
    return ENVIRONMENTS[env]["keys_filename"]


def trusted_jwks(env: str = "prod") -> dict:
    return ENVIRONMENTS[env]["jwks"]


def metadata_url(env: str = "prod") -> str:
    return ENVIRONMENTS[env]["metadata_url"]
"""Helper file."""
import json


def read_creds():
    """Read credentials file."""
    with open("credentials.json") as json_file:
        data = json.load(json_file)
    return data
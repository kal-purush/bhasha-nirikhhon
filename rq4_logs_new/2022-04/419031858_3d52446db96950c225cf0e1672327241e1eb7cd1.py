from ipaddress import IPv4Address
import os


class MongoDBSettings:
    address: IPv4Address
    port: int
    user: str
    password: str
    db_name: str

    def __init__(self):
        self.address = IPv4Address(os.environ.get('LINKURATOR_DB_ADDRESS', '127.0.0.1'))
        self.port = int(os.environ.get('LINKURATOR_DB_PORT', 27017))
        self.db_name = os.environ.get('LINKURATOR_DB_NAME', 'main')
        self.user = os.environ.get('LINKURATOR_DB_USER', '')
        self.password = os.environ.get('LINKURATOR_DB_PASSWORD', '')
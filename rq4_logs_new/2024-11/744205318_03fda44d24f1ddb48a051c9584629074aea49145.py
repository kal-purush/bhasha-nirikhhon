import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Settings, cls).__new__(cls)
            cls._instance._init_instance()
        return cls._instance

    def _init_instance(self):
        self.MG_HOST = os.getenv("MG_HOST")
        self.MG_PORT = os.getenv("MG_PORT")

settings = Settings()
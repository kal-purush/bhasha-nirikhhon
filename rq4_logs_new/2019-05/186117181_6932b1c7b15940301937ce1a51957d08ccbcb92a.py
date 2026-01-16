import os


class Config(object):
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.getenv('SECRET_KEY')
    SQLALCHEMY_DATABASE_URI = os.environ.get('APP_DATABASE')


class Development(Config):
    DEBUG = True


class Testing(Config):
    TESTING = True
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = os.environ.get('TEST_DATABASE')


class Production(Config):
    DEBUG = False


config = {
    'development': Development,
    'production': Production,
    'testing': Testing
}
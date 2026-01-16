class DevelopmentConfig(object):
    DEBUG = True


class ProductionConfig(object):
    DEBUG = False

app_config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig
}
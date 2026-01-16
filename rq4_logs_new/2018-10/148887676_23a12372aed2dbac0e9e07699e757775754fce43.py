
DEBUG      = False
DEBUG_LOG  = False

'''
数据库的配置
'''
DB_HOST    = ''
DB_PORT    = 3306
DB_USER    = 'root'
DB_PWD     = ''
DB_NAME    = ''
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(DB_USER,DB_PWD,DB_HOST,DB_PORT,DB_NAME)
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_ECHO = True

'''
缓存信息配置
'''
REDIS_HOST  = ''
REDIS_PORT  = 6379
REDIS_DB    = 0
REDIS_PWD   = ''
REDIS_EX    = 24 * 60 * 60
# CACHE_TYPE  = 'simple'
# REDIS_URL = "redis://:{}@{}:{}/{}".format(REDIS_PWD, REDIS_HOST, REDIS_PORT, REDIS_DB)


SLEEP_TIME = 1 * 60
LOGS_DIR = "./logData/"

DINGDING_MESSAGE = 'https://oapi.dingtalk.com/robot/send?access_token=e7eee758ba35c26681c215ac650f972b95614ade270e6984f5e2759310ab9ea6'
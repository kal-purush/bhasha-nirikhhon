from app.main import app
import sys
import logging
from logging.handlers import RotatingFileHandler
 
if __name__ == "__main__":
        formatter = logging.Formatter( "%(message)s ")
        handler = RotatingFileHandler('logs/mylog.log', maxBytes=10000, backupCount=5,mode='a')
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        app.logger.addHandler(handler)
        app.logger.addHandler(logging.StreamHandler(stream=sys.stdout))
        app.logger.setLevel(logging.DEBUG)
        app.run(host='0.0.0.0',debug=False)
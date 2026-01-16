from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
#import sys

app = Flask(__name__)
app.config.from_object('config')
db = SQLAlchemy(app)

#print sys.path
#sys.path.append("/Users/kano/Documents/cookpad/")
from app import views, models

import logging
logger = logging.getLogger('flacahn')
hdlr = logging.FileHandler('../myapp.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.WARNING)
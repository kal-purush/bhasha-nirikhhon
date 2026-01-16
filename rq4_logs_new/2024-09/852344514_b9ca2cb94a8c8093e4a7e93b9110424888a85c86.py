import cloudinary
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from urllib.parse import quote
from flask_login import LoginManager


app = Flask(__name__)

app.secret_key = '@#$%^&#@$%^$%^&*@#$%^&43567*#$%^&*'

app.config["SQLALCHEMY_DATABASE_URI"] = 'mysql+pymysql://root:%s@localhost/nhom07?charset=utf8mb4' % quote('Huytan@2304')


app.config['CART_KEY'] = 'cart'
app.config['DATE_KEY'] = 'date'
app.config['ORDERER_KEY'] = 'orderer'
app.config['DETAILS_KEY'] = 'details'
app.config['S_INFO_KEY'] = 'info'
app.config['S_DETAILS_KEY'] = 's_details'

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True

cloudinary.config(
    cloud_name="dqec4llav",
    api_key="752187729553174",
    api_secret="LPw7aj9WseIgRmVct7bdppxfa5g"
)



db = SQLAlchemy(app=app)
login = LoginManager(app=app)
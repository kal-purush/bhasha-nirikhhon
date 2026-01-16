import pymysql
import server.config.settings as AppSettings
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = AppSettings.dbConnectionString
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

db = SQLAlchemy(app)

'''
@app.route('/')
def testdb():
    try:
        db.session.query_property("1").
        db.session.query("1").from_statement("SELECT 1").all()
        return '<h1>It works.</h1>'
    except:
        return '<h1>Something is broken.</h1>'

def index():
    return "Hello, World!"

if __name__ == '__main__':
    app.run(debug=True)
'''
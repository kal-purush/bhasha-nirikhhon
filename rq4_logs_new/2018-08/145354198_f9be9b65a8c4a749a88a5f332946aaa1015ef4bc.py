from flask import Flask
from flask import jsonify
from flask import request, jsonify
from flask_pymongo import PyMongo
from flask import render_template
import json 
from bson.objectid import ObjectId 


app = Flask(__name__)

app.config['MONGO_DBNAME'] = 'mars'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/mars'

mongo =PyMongo(app)


@app.route('/')
def index():
    m = mongo.db.m_data.find_one(
   
  )
    
    return render_template("index.html", m=m) 


app.run(debug=True, port=5545)
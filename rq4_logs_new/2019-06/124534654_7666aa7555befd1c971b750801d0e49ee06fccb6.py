#!/usr/bin/python

import MySQLdb
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def home():
    db = MySQLdb.connect(host="host03",user="myuser",db="mydb",passwd="mypassword")

    cursor = db.cursor()
    query = "SELECT * FROM limbs"
    cursor.execute(query)

    results = cursor.fetchall()

    return render_template("index.html", rows=results)

if __name__ == '__main__':
    app.run(debug=False,host='0.0.0.0')
from flask import Flask
from flask import render_template
from flask.ext.sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sites.db'
db = SQAlchemy(app)

class Site(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    address  = db.Column(db.String, unique=True)
    status = db.Column(db.Enum('online', 'offline'))

    def __repr__(self):
        return '<Address: {0.address}, Status: {0.status}>'.format(self)

@app.route('/')
def index():
    return "Hello world"

@app.route('/sites')
def checks():
    sites = [{"name":"www.wp.pl", "status": "down"},
            {"name":"www.google.pl", "status": "up"}
            ]
    return render_template("sites.html", sites=sites)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
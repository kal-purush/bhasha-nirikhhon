from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_sqlalchemy import SQLAlchemy


# Mysql Connection
app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:password@localhost/dgtldispenser'


app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

#settings
app.secret_key = 'mysecretkey'

class Alarm(db.Model):
    __tablename__ = 'tablealarm'
    id = db.Column(db.Integer, primary_key=True)
    hours = db.Column(db.String(200))
    minutes = db.Column(db.String(200))
    seconds = db.Column(db.String(200))
    day = db.Column(db.String(200))
    pill1 = db.Column(db.Integer)
    pill2 = db.Column(db.Integer)
    pill3 = db.Column(db.Integer)

    def __init__(self, hours, minutes, seconds, day, pill1, pill2, pill3):
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.day = day
        self.pill1 = pill1
        self.pill2 = pill2
        self.pill3 = pill3

#db.create_all()


# settings
#app.secret_key = 'mysecretkey'

@app.route('/')
def Index():
    all_alarms = Alarm.query.all()
    
    return render_template('index.html', data = all_alarms )

@app.route('/add_alarm', methods=['POST'])
def add_alarm():
 if request.method == 'POST':
    hours = request.form['hours']
    minutes = request.form['minutes']
    seconds = request.form['seconds']
    day = request.form['day']
    pill1 = request.form['pill1']
    pill2 = request.form['pill2']
    pill3 = request.form['pill3']

    new_alarm = Alarm(hours, minutes, seconds, day, pill1, pill2, pill3)
    db.session.add(new_alarm)
    db.session.commit()

    flash('Alarm Added successfully')
    return redirect(url_for('Index'))
   
      
@app.route('/update/<id>', methods = ['GET', 'POST', 'PUT'])
def update_alarm(id):
 if request.method == 'POST':

    #alarm = Alarm.query.get(request.form.get('id'))
    alarm = Alarm.query.get(id)
    
    alarm.hours = request.form['hours']
    alarm.minutes = request.form['minutes']
    alarm.seconds = request.form['seconds']
    alarm.day = request.form['day']
    alarm.pill1 = request.form['pill1']
    alarm.pill2 = request.form['pill2']
    alarm.pill3 = request.form['pill3']

    db.session.commit()
    
    flash('Alarm Updated Successfully')
    return redirect(url_for('Index'))

@app.route('/edit/<id>')
def get_alarm(id):

    data = Alarm.query.get(id)
    return render_template('edit.html', alarm = data)

@app.route('/delete/<string:id>/')
def delete_alarm(id):
  
   alarm = Alarm.query.get(id)
   db.session.delete(alarm)
   db.session.commit()
   flash('Alarm Removed Successfully')
   return redirect(url_for('Index'))

if __name__ == '__main__':
 app.run(port = 3000, debug = True)
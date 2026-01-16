import json

from flask import Flask, render_template,url_for

app = Flask(__name__)


@app.route('/')
def index():
    with open("goals.json", "r", encoding='utf-8') as f:
        goals = json.load(f)
    with open("teachers.json", "r", encoding='utf-8') as f:
        teachers = json.load(f)
    return render_template('flask_teacher/index.html',goals=goals,teachers=teachers)


@app.route('/all/')
def all_teachers():
    return render_template('flask_teacher/all.html')


@app.route('/goals/<goal>/')
def goals(goal):
    return render_template('flask_teacher/goal.html')


@app.route('/profiles/<int:pk>/')
def profiles(pk):
    return render_template('flask_teacher/profile.html')


@app.route('/request/')
def order_request():
    return render_template('flask_teacher/request.html')


@app.route('/request_done/')
def order_accepted():
    return render_template('flask_teacher/request_done.html')


@app.route('/booking/<int:pk>/')
def booking(pk):
    return render_template('flask_teacher/booking.html')


@app.route('/booking_done/')
def booking_accepted():
    return render_template('flask_teacher/booking_done.html')


if __name__ == '__main__':
    app.run(debug=True)
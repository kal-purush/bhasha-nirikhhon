from flask import Flask, jsonify, request, render_template
from data import *

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/schools')
def get_all_schools():
    return jsonify({'schools': schools})


@app.route('/school/<string:name>')
def get_school(name):
    for school in schools:
        if school['name'] == name:
            return jsonify(school)
    return jsonify({'Error': 'school not found'})


@app.route('/school', methods=['POST'])
def add_school():
    request_data = request.get_json()
    new_school = {
        'name': request_data['name'],
        'students': [
        ]
    }
    schools.append(new_school)
    return jsonify(new_school)


@app.route('/school/<string:name>/students')
def get_students(name):
    for school in schools:
        if school['name'] == name:
            return jsonify(school['students'])
    return jsonify({'Error': 'school not found'})


@app.route('/school/<string:name>/student', methods=['POST'])
def add_student(name):
    data = request.get_json()
    for school in schools:
        if school['name'] == name:
            new_student = {'first_name': data['first_name'], 'last_name': data['last_name'], 'GPA': data['GPA'], 'Year': data['Year']}
            store['students'].append(new_student)
            return jsonify(new_student)
    return jsonify({'Error': 'school not found'})
    

app.run(port=3030, debug=True)
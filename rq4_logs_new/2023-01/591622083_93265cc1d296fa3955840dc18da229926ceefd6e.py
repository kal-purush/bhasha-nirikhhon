#Create a REST API using Python and a SQLAlchemy database that allows users to manage a list of tasks. The API should have the following endpoints:
#    GET /tasks: Retrieve a list of all tasks in the database
#    POST /tasks: Add a new task to the database
#    PUT /tasks/<id>: Update an existing task in the database
#   DELETE /tasks/<id>: Delete a task from the database

#The task model should have the following fields:

    # id: Unique identifier for the task (integer)
    # title: Title of the task (string)
    # author: Author of the task (string)
    # year: Year the task was published (integer)
    # isbn: ISBN number of the task (string)

    #The API should be able to handle validation errors, such as a missing required field or an invalid data type for a field.
    




from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import os

# Init app
app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
# Database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db.sqlite')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Init db
db = SQLAlchemy(app)

# Init ma
ma = Marshmallow(app)

# Task Class/Model
class Task(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), unique=True)
    author = db.Column(db.String(100))
    year = db.Column(db.Integer)
    isbn = db.Column(db.String(13))

    def __init__(self, title, author, year, isbn):
        self.title = title
        self.author = author
        self.year = year
        self.isbn = isbn
    
# Task Schema
class TaskSchema(ma.Schema):
    class Meta:
        fields = ('id', 'title', 'author', 'year', 'isbn')

# Init schema
task_schema = TaskSchema()
tasks_schema = TaskSchema(many=True)

# Create a Task
@app.route('/task', methods=['POST'])
def add_task():
    title = request.json['title']
    author = request.json['author']
    year = request.json['year']
    isbn = request.json['isbn']

    new_task = Task(title, author, year, isbn)

    db.session.add(new_task)
    db.session.commit()

    return task_schema.jsonify(new_task)

# Get All Tasks
@app.route('/task', methods=['GET'])   
def get_tasks():
    all_tasks = Task.query.all()
    result = tasks_schema.dump(all_tasks)
    return jsonify(result)

# Get Single Task
@app.route('/task/<id>', methods=['GET'])
def get_task(id):
    task = Task.query.get(id)
    return task_schema.jsonify(task)

# Update a Task
@app.route('/task/<id>', methods=['PUT'])
def update_task(id):
    task = Task.query.get(id)

    title = request.json['title']
    author = request.json['author']
    year = request.json['year']
    isbn = request.json['isbn']

    task.title = title
    task.author = author
    task.year = year
    task.isbn = isbn

    db.session.commit()

    return task_schema.jsonify(task)

# Delete Task
@app.route('/task/<id>', methods=['DELETE'])
def delete_task(id):
    task = Task.query.get(id)
    db.session.delete(task)
    db.session.commit()

    return task_schema.jsonify(task)

# Run Server
if __name__ == '__main__':
    app.run(debug=True)




#!flask/bin/python
from flask import Flask, jsonify, abort, make_response, request

app = Flask(__name__)

 # https://github.com/adafruit/adafruit-beaglebone-io-pyth

analog_in = {
    # UNIVERSAL IN (Analog)
    'UI1': "P9_39",
    'UI2': "P9_40",
    'UI3': "P9_37",
    'UI4': "P9_38",
    'UI5': "P9_33",
    'UI6': "P9_36",
    'UI7': "P9_35",
}

analog_out = {
    # UNIVERSAL OUT (Analog)
    'UO1': "P8_13",
    'UO2': "P8_19",
    'UO3': "P9_42",

    # ANALOG OUT (Analog)
    'AO1': "P9_14",
    'AO2': "P9_16",
}

digital_in = {
    # DIGITAL IN (Digital)
    'DI1': "P8_7",
    'DI2': "P9_12",
    'DI3': "P9_15",
    'DI4': "P9_18",
}

digital_out = {
    # RELAY (Digital)
    'R1': "P8_16",
    'R2': "P8_17",
    'R3': "P8_18",
    'R4': "P8_26",
}



tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol', 
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web', 
        'done': False
    }
]

@app.route('/todo/api/v1.0/tasks', methods=['GET'])
def get_tasks():
    return jsonify({'tasks': tasks})


@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods=['GET'])
def get_task(task_id):
    task = [task for task in tasks if task['id'] == task_id]
    if len(task) == 0:
        abort(404)
    return jsonify({'task': task[0]})

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.route('/todo/api/v1.0/tasks', methods=['POST'])
def create_task():
    if not request.json or not 'title' in request.json:
        abort(400)
    task = {
        'id': tasks[-1]['id'] + 1,
        'title': request.json['title'],
        'description': request.json.get('description', ""),
	#'description':request.json['description'],
        'done': False
    }
    tasks.append(task)
    return jsonify({'task': task}), 201

@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods=['PUT'])
def update_task(task_id):
    task = [task for task in tasks if task['id'] == task_id]
    if len(task) == 0:
        abort(404)
    if not request.json:
        abort(400)
    if 'title' in request.json and type(request.json['title']) != unicode:
        abort(400)
    if 'description' in request.json and type(request.json['description']) is not unicode:
        abort(400)
    if 'done' in request.json and type(request.json['done']) is not bool:
        abort(400)
    task[0]['title'] = request.json.get('title', task[0]['title'])
    task[0]['description'] = request.json.get('description', task[0]['description'])
    task[0]['done'] = request.json.get('done', task[0]['done'])
    return jsonify({'task': task[0]})

@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    task = [task for task in tasks if task['id'] == task_id]
    if len(task) == 0:
        abort(404)
    tasks.remove(task[0])
    return jsonify({'result': True})


if __name__ == '__main__':
    app.run(host='0.0.0.0')
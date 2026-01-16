from flask import Flask, request, render_template
from flask_socketio import SocketIO
import json
import threading
import time
import signal


app = Flask(__name__)
socketio = SocketIO(app)

terminate_thread = False
pending_tasks = []
connected_workers = dict()

@socketio.on('connect')
def socket_connect():
    connected_workers[request.sid] = False
    print(connected_workers)

@socketio.on('disconnect')
def socket_connect():
    connected_workers.pop(request.sid)
    print(connected_workers)

@socketio.on('submit_result')
def submit_result(data):
    print(data)
    connected_workers[request.sid] = False


@app.route("/api/runcode", methods=['POST'])
def runcode():
    data = json.loads(request.get_data())
    pending_tasks.append(data)
    return ""

def TaskBroker():
    while not terminate_thread:
        if (len(pending_tasks) > 0):
            for id, working in connected_workers.items():
                if not working:
                    socketio.emit("runcode", pending_tasks.pop(), to=id)
                    connected_workers[id] = True
                    break
        else:
            time.sleep(.1)

def handle_interrupt(signum, frame):
    global terminate_thread
    print("Code Runner API Terminating...")
    terminate_thread = True
    exit(0)
signal.signal(signal.SIGINT, handle_interrupt)
signal.signal(signal.SIGTERM, handle_interrupt)

task_broker_thread = threading.Thread(target=TaskBroker)
task_broker_thread.start()

socketio.run(app, port=8989)
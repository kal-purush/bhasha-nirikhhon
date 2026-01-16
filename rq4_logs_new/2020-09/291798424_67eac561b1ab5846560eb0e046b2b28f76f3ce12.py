import os
import time
import psutil
from flask import Flask

app = Flask(__name__)

STALE = 12345678910111213;

@app.route('/')
def hello_world():
    t_end = time.time() + 2
    while time.time() < t_end:
        STALE*STALE
    return "Request finished. CPU load at the end of request: %s \n" % psutil.cpu_percent()

@app.route('/health')
def healtchcheck():
    return "OK"
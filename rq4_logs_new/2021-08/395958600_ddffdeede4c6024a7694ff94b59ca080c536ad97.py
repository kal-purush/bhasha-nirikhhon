import inspect
import time
from flask import Flask
app = Flask(__name__)


def foo():
    print(f'start running: foo')
    time.sleep(1)
    print(f'stop running: foo')


def bar():
    print(f'start running: bar')
    time.sleep(2)
    foo()
    print(f'stop running: bar')


@app.route('/')
def hello_world():
    bar()
    return 'Hello World'

if __name__ == '__main__':
    #To profile flask
    from werkzeug.middleware.profiler import ProfilerMiddleware
    app.wsgi_app = ProfilerMiddleware(app.wsgi_app, profile_dir=".")
    app.run('127.0.1.1', '8888')
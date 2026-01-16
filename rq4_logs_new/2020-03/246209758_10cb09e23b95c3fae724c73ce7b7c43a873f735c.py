from flask import Flask
import time

app = Flask(__name__)

i = 0

@app.route('/')
def hello_world():
    # time.sleep(30)
    global i
    i = i+1
    if (i > 2):
        return "index is " + str(i) + " so fail", 400
    return 'Hello world: ' + str(i)

@app.route('/count')
def count():
    return i

if __name__ == '__main__':
    app.run(host='0.0.0.0')
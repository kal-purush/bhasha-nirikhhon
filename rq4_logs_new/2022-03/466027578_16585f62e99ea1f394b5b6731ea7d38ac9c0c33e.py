
from flask import Flask
app = Flask(__name__)

@app.route('/live_check')
def hello_world():
    return 'Running'

if __name__ == "__main__":
   app.run(host="localhost", port=4000)
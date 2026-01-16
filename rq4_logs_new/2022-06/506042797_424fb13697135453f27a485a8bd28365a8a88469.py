# 1. Import Flask
from flask import Flask
# 2. Create an app
app = Flask(__name__)
# 3. 
@app.route('/')
def hello_world():
    return 'Hello world'
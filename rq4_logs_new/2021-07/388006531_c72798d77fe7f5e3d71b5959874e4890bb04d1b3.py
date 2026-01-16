from flask import Flask
from flask import request
 
app = Flask(__name__)
 
@app.route("/")
def index():
	val = request.args.get("var")
 
	return "Hello, World! {}".format(val)
 
if __name__=="__main__":
	app.run()
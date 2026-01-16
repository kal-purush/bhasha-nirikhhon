import flask
import datetime
app = flask.Flask(__name__)
app.config["DEBUG"] = True
@app.route('/', methods=['GET'])
def home():
        b = datetime.datetime.now().astimezone().strftime("%a %b %y %X %Z %Y")
        y = {
            "date" :b
        }
        return y
app.run()
from flask import Flask,render_template


# this makes app an instance of the Flask class
# and it passed the special variable __name__ into
# the constructor

app = Flask(__name__)

@app.route("/")
def help():
    return render_template("index.html")


if __name__=="__main__":
    app.debug=True
    app.run() 
    #app.run(host="0.0.0.0",port=8888)
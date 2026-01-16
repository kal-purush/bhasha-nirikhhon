from flask import Flask
import os
import socket


app = Flask(__name__)

@app.route("/")
def hello():
    html = "<h3>Hello {name}!</h3>" \
           "<b>Hostname:</b> {hostname}<br/>"

    return html.format(name=os.getenv("NAME", "world"), hostname=socket.gethostname())

@app.route("/loaderio-7199d3794c4139acd6994e9b45eab8ff")
def loader():
    html = "loaderio-7199d3794c4139acd6994e9b45eab8ff"

    return html.format()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
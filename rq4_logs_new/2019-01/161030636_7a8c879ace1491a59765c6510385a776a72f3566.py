from flask import Flask, render_template
import requests

app = Flask(__name__)

def get_data():
    url = "http://ws.audioscrobbler.com/2.0/?method=chart.gettoptracks&api_key=6c08b2f4e48e2262e18b4e1829dabb86&format=json"
    r = requests.get(url)
    return r.json()

#print(get_data())
#

@app.route('/')
def main_page():
    return render_template("1.html", data=get_data())

if __name__ == '__main__':
    app.run()
